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

package com.ibm.stocator.fs.swift;

import java.io.EOFException;
import java.io.IOException;

import com.ibm.stocator.fs.common.Constants;
import com.ibm.stocator.fs.common.exception.ClientException;
import com.ibm.stocator.fs.swift.auth.JossAccount;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SwiftInputStream extends FSInputStream implements CanSetReadahead {

  /**
   * current position
   */
  private long pos;
  /**
   * is connection closed
   */
  private volatile boolean closed;
  /**
   * Wrap of the input stream
   */
  private SwiftInputStreamWrapper wrappedStream;
  /**
   * Overall content length
   */
  private final long contentLength;
  /**
   * Data uri
   */
  private final String uri;
  /**
   * Logger
   */
  private static final Logger LOG = LoggerFactory.getLogger(SwiftInputStream.class);
  /**
   * Read strategy
   */
  private final String readStrategy;
  /**
   * Read ahead value
   */
  private long readahead;
  /**
   * Next reading position
   */
  private long nextReadPos;

  /**
   * Pointer to the end of the last range
   */
  private long contentRangeFinish;

  /**
   * Pointer to the start of the last range
   */
  private long contentRangeStart;
  /**
   * Wrapper for Joss Account
   */
  private JossAccount mJossAccount;

  /**
   * Default constructor
   *
   * @param pathT data path
   * @param contentLengthT object size
   * @param jossAccountT joss client
   * @param readStrategyT read strategy
   */
  public SwiftInputStream(String pathT, long contentLengthT, JossAccount jossAccountT,
      String readStrategyT) throws IOException {
    contentLength = contentLengthT;
    mJossAccount = jossAccountT;

    uri = pathT;
    readStrategy = readStrategyT;
    readahead = Constants.DEFAULT_READAHEAD_RANGE;
    setReadahead(readahead);

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

    contentRangeFinish = getReadLimit(readStrategy, targetPos, length, contentLength,
        readahead);
    LOG.trace("reopen({}) for {} range[{}-{}], length={},"
        + " streamPosition={}, nextReadPosition={}", uri, msg,
        targetPos, contentRangeFinish, length, pos, nextReadPos);

    try {
      wrappedStream = SwiftAPIDirect.getObject(new Path(uri),
          mJossAccount, targetPos, contentRangeFinish);
      contentRangeStart = targetPos;
      if (wrappedStream == null) {
        throw new IOException("Null IO stream from reopen of (" + msg + ") " + uri);
      }
    } catch (ClientException e) {
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
    checkNotClosed();

    // Do not allow negative seek
    if (targetPos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK + " " + targetPos);
    }

    if (contentLength <= 0) {
      return;
    }
    nextReadPos = targetPos;
  }

  private void seekWithoutException(long positiveTargetPos) {
    try {
      seek(positiveTargetPos);
    } catch (IOException ioe) {
      LOG.debug("Ignoring IOE on seek of {} to {}", uri, positiveTargetPos, ioe);
    }
  }

  private void seekInStream(long targetPos, long length) throws IOException {
    checkNotClosed();
    if (wrappedStream == null) {
      return;
    }
    // compute how much more to skip
    long diff = targetPos - pos;
    if (diff > 0) {
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

  private void lazySeek(long targetPos, long len) throws IOException {
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
    checkNotClosed();
    if (contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }
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
    checkNotClosed();
    if (len == 0) {
      return 0;
    }
    if (contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
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
      onReadFailure(e, len);
      return -1;
    } catch (IOException e) {
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
        LOG.trace("Stream closed");
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
        } catch (IOException e) {
          LOG.debug("When closing {} stream for {}", uri, msg, e);
          shouldAbort = true;
        }
      }
      if (shouldAbort) {
        wrappedStream.abort();
      }
      LOG.trace("Stream {} {}: {}; streamPos={}, nextReadPos={},"
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
   * Bytes left in stream.
   *
   * @return how many bytes are left to read
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInFile() {
    return contentLength - pos;
  }

  /**
   * How many bytes has left in the request
   *
   * @return how many bytes are left to read in the current GET
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInCurrentRequest() {
    return contentRangeFinish - pos;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long getContentRangeFinish() {
    return contentRangeFinish;
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
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

  /**
   * Get read limit
   *
   * @param readStrategy read strategy
   * @param targetPos position
   * @param length length
   * @param contentLength total size
   * @param readahead read ahead value
   * @return range limit
   */
  static long getReadLimit(String readStrategy, long targetPos, long length,
      long contentLength, long readahead) {
    long rangeLimit;
    switch (readStrategy) {
      case Constants.RANDOM_READ_STRATEGY:
        rangeLimit = (length < 0) ? contentLength : targetPos + Math.max(readahead, length);
        break;
      case Constants.SEQ_READ_STRATEGY:
        rangeLimit = contentLength;
        break;
      case Constants.NORMAL_READ_STRATEGY:
      default:
        rangeLimit = contentLength;
    }
    rangeLimit = Math.min(contentLength, rangeLimit);
    return rangeLimit;
  }
}
