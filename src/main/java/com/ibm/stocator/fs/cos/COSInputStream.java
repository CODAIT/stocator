/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.stocator.fs.cos;

import java.io.EOFException;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.ibm.stocator.fs.common.Constants;

import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class COSInputStream extends FSInputStream implements CanSetReadahead {

  /*
   * Client for operations with COS
   */
  private AmazonS3 mClient;

  /*
   * Name of the bucket the object resides in
   */
  private final String mBucketName;

  /*
   * The path of the object to read
   */
  private final String mKey;

  /*
   * The backing input stream from COS
   */
  private S3ObjectInputStream wrappedStream;
  /*
   * COS object
   */
  private S3Object cosObject;
  /*
   * current position
   */
  private long pos;
  /*
   * is connection closed
   */
  private volatile boolean closed;
  /*
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(COSInputStream.class);
  /*
   * Read ahead value
   */
  private long readahead;
  /*
   * Next reading position
   */
  private long nextReadPos;
  /*
   * Pointer to the end of the last range
   */
  private long contentRangeFinish;
  /*
   * Pointer to the start of the last range
   */
  private long contentRangeStart;
  /*
   * Read threashold
   */
  private final long threasholdRead = 65536;
  /*
   * negative seek
   */
  private long negativeSeek = 0;
  /*
   * content length
   */
  private long contentLength = -1;
  /*
   * uri
   */
  private String uri;

  /**
   * Default constructor
   *
   * @param bucketT bucket
   * @param keyT key
   * @param readAheadT read strategy
   * @param client COS client
   */
  public COSInputStream(String bucketT, String keyT,long readAheadT, AmazonS3 client) {
    mBucketName = bucketT;
    mKey = keyT;
    mClient = client;
    readahead = readAheadT;
    setReadahead(readahead);
    uri = mBucketName + mKey;
  }

  /**
   * Reopen stream if closed
   *
   * @param msg Details of reopen stream
   * @param targetPos target position
   * @param length length
   * @throws IOException if error
   */
  private synchronized void reopen(String msg, long targetPos, long length) throws IOException {
    if (wrappedStream != null) {
      closeStream("reopen(" + msg + ")", contentRangeFinish);
    }
    contentRangeStart = targetPos;
    contentRangeFinish = targetPos + Math.max(readahead, length) + threasholdRead;
    if (negativeSeek < 0) {
      contentRangeFinish = targetPos + Math.abs(negativeSeek);
      negativeSeek = 0;
    }
    try {
      LOG.trace("reopen({}) for {} range[{}-{}], length={},"
          + " streamPosition={}, nextReadPosition={}", uri, msg,
          contentRangeStart, contentRangeFinish, length, pos, nextReadPos);

      GetObjectRequest request =
          new GetObjectRequest(mBucketName, mKey).withRange(contentRangeStart, contentRangeFinish);
      cosObject = mClient.getObject(request);
      LOG.trace("Got COS object {}", uri);
      wrappedStream = cosObject.getObjectContent();
      contentLength = cosObject.getObjectMetadata().getContentLength();
      if (wrappedStream == null) {
        cosObject.close();
        throw new IOException("Null IO stream from reopen of (" + msg + ") " + uri);
      }
    } catch (AmazonClientException e) {
      LOG.error(e.getMessage());
      throw new IOException("Reopen at position " + targetPos + uri);
    }
    pos = targetPos;
  }

  @Override
  public synchronized long getPos() throws IOException {
    return (nextReadPos < 0) ? 0 : nextReadPos;
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    LOG.trace("seek {} to {}", uri, targetPos);
    checkNotClosed();
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
    }
    nextReadPos = targetPos;
  }

  /**
   * Seek without throwing exception
   *
   * @param positiveTargetPos position
   */
  private void seekWithoutException(long positiveTargetPos) {
    LOG.trace("seek without exception {}", positiveTargetPos);
    try {
      seek(positiveTargetPos);
    } catch (IOException ioe) {
      LOG.debug("Ignoring IOE on seek of {} to {}", uri, positiveTargetPos, ioe);
    }
  }

  /**
   * Seek in stream
   *
   * @param targetPos target position
   * @param length length
   * @throws IOException if error
   */
  private void seekInStream(long targetPos, long length) throws IOException {
    checkNotClosed();
    if (wrappedStream == null) {
      return;
    }
    // compute how much more to skip
    long diff = targetPos - pos;
    if (diff > 0) {
      LOG.trace("seekInStream: {}, forward seek to {}", uri, diff);
      // forward seek -this is where data can be skipped

      int available = wrappedStream.available();
      // always seek at least as far as what is available
      long forwardSeekRange = Math.max(readahead, available);
      // work out how much is actually left in the stream
      // then choose whichever comes first: the range or the EOF
      long remainingInCurrentRequest = remainingInCurrentRequest();

      long forwardSeekLimit = Math.min(remainingInCurrentRequest, forwardSeekRange);
      boolean skipForward = remainingInCurrentRequest > 0 && diff <= forwardSeekLimit;
      if (skipForward) {
        // the forward seek range is within the limits
        LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
        long skipped = wrappedStream.skip(diff);
        if (skipped > 0) {
          pos += skipped;
          // as these bytes have been read, they are included in the counter
        }

        if (pos == targetPos) {
          // all is well
          return;
        } else {
          // log a warning; continue to attempt to re-open
          LOG.warn("Failed to seek on {} to {}. Current position {}", uri, targetPos, pos);
        }
      }
    } else if (diff < 0) {
      // backwards seek
      LOG.trace("seekInStream: {} backward seek {}", uri, diff);
      negativeSeek = diff;
    } else {
      // targetPos == pos
      if (remainingInCurrentRequest() > 0) {
        // if there is data left in the stream, keep going
        return;
      }
    }
    // if the code reaches here, the stream needs to be reopened.
    // close the stream; if read the object will be opened at the new pos
    closeStream("seekInStream()", contentRangeFinish);
    pos = targetPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * lazy seek in stream
   *
   * @param targetPos target position
   * @param len length
   * @throws IOException if error
   */
  private void lazySeek(long targetPos, long len) throws IOException {
    LOG.trace("Lazy seek call {}, target {}, len {}", uri, targetPos, len);
    seekInStream(targetPos, len);
    if (wrappedStream == null) {
      reopen("read from new offset", targetPos, len);
    }
  }

  private void onReadFailure(IOException ioe, int length) throws IOException {
    LOG.info("Got exception while trying to read from stream {}" + " trying to recover: " + ioe,
        uri);
    LOG.debug("While trying to read from stream {}", uri, ioe);
    reopen("failure recovery", pos, length);
  }

  @Override
  public synchronized int read() throws IOException {
    LOG.trace("read 1 byte from {}", uri);
    checkNotClosed();
    int byteRead;
    try {
      lazySeek(nextReadPos, 1);
      byteRead = wrappedStream.read();
    } catch (EOFException e) {
      return -1;
    } catch (IOException e) {
      onReadFailure(e, 1);
      byteRead = wrappedStream.read();
    }
    if (byteRead >= 0) {
      pos++;
      nextReadPos++;
    }
    return byteRead;
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    LOG.trace("read {}, off {}, len {}", uri, off, len);
    checkNotClosed();
    if (len == 0) {
      return 0;
    }
    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      return -1;
    }

    int bytesRead;
    try {
      bytesRead = wrappedStream.read(buf, off, len);
    } catch (EOFException e) {
      LOG.trace(e.getMessage());
      onReadFailure(e, len);
      return -1;
    } catch (IOException e) {
      LOG.trace(e.getMessage());
      onReadFailure(e, len);
      bytesRead = wrappedStream.read(buf, off, len);
    }

    if (bytesRead > 0) {
      pos += bytesRead;
      nextReadPos += bytesRead;
    }
    return bytesRead;
  }

  private void checkNotClosed() throws IOException {
    if (closed) {
      LOG.debug("closed {}. Throw exception", uri);
      throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        closeStream("close() operation", contentRangeFinish);
        super.close();
      } finally {
        LOG.trace("{}. Stream closed", uri);
      }
    }
  }

  /**
   * close the stream
   *
   * @param msg close message
   * @param length length
   */
  private void closeStream(String msg, long length) {
    if (wrappedStream != null) {
      long remaining = remainingInCurrentRequest();
      boolean shouldAbort = remaining > readahead;
      if (!shouldAbort) {
        try {
          wrappedStream.close();
          cosObject.close();
        } catch (IOException e) {
          LOG.debug("When closing {} stream for {}", uri, msg, e);
          shouldAbort = true;
        }
      }
      if (shouldAbort) {
        LOG.trace("Abort {}", uri);
        wrappedStream.abort();
      }
      LOG.trace("Close stream {} {}: {}; streamPos={}, nextReadPos={},"
          + " request range {}-{} length={}", uri, (shouldAbort ? "aborted" : "closed"), msg,
          pos, nextReadPos, contentRangeStart, contentRangeFinish, length);
      wrappedStream = null;
    }
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = remainingInFile();
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int) remaining;
  }

  /**
   * Bytes left in stream
   *
   * @return how many bytes are left to read
   * @throws IOException if error
   */
  public synchronized long remainingInFile() throws IOException {
    return contentLength - pos;
  }

  /**
   * How many bytes has left in the request
   *
   * @return how many bytes are left to read in the current GET
   */
  public synchronized long remainingInCurrentRequest() {
    return contentRangeFinish - pos;
  }

  public synchronized long getContentRangeFinish() {
    return contentRangeFinish;
  }

  public synchronized long getContentRangeStart() {
    return contentRangeStart;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    checkNotClosed();
    if (length == 0) {
      return;
    }
    int nread = 0;
    synchronized (this) {
      long oldPos = getPos();
      try {
        seek(position);
        while (nread < length) {
          int nbytes = read(buffer, offset + nread, length - nread);
          if (nbytes < 0) {
            throw new EOFException("EOF in read fully method");
          }
          nread += nbytes;
        }
      } finally {
        seekWithoutException(oldPos);
      }
    }
  }

  @Override
  public synchronized void setReadahead(Long readaheadT) {
    if (readaheadT == null) {
      readahead = Constants.DEFAULT_READAHEAD_RANGE;
    } else {
      readahead = readaheadT;
    }
  }

  public synchronized long getReadahead() {
    return readahead;
  }
}
