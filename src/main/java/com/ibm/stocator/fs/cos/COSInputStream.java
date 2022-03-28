/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.cos;

import java.io.EOFException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.cloud.objectstorage.AmazonClientException;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.GetObjectRequest;
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectInputStream;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;

public class COSInputStream extends FSInputStream implements CanSetReadahead {
  /**
   * This is the public position; the one set in {@link #seek(long)}
   * and returned in {@link #getPos()}.
   */
  private long pos;
  /**
   * Closed bit. Volatile so reads are non-blocking.
   * Updates must be in a synchronized block to guarantee an atomic check and
   * set
   */
  private volatile boolean closed;
  private S3ObjectInputStream wrappedStream;
  private final AmazonS3 client;
  private final String bucket;
  private final String key;
  private final long contentLength;
  private final String uri;
  private static final Logger LOG =
      LoggerFactory.getLogger(COSInputStream.class);
  private final COSInputPolicy inputPolicy;
  private long readahead = COSConstants.DEFAULT_READAHEAD_RANGE;

  /**
   * This is the actual position within the object, used by
   * lazy seek to decide whether to seek on the next read or not.
   */
  private long nextReadPos;

  /**
   * The end of the content range of the last request.
   * This is an absolute value of the range, not a length field.
   */
  private long contentRangeFinish;

  /**
   * The start of the content range of the last request.
   */
  private long contentRangeStart;
  private Statistics stats;

  public COSInputStream(String bucketT, String keyT,
      long contentLengthT,
      AmazonS3 clientT,
      long readahead,
      COSInputPolicy inputPolicyT,
      Statistics statisticsT) {
    bucket = bucketT;
    key = keyT;
    contentLength = contentLengthT;
    client = clientT;
    uri = bucket + "/" + key;
    inputPolicy = inputPolicyT;
    stats = statisticsT;
    setReadahead(readahead);
  }

  /**
   * Opens up the stream at specified target position and for given length.
   *
   * @param reason reason for reopen
   * @param targetPos target position
   * @param length length requested
   * @throws IOException on any failure to open the object
   */
  private synchronized void reopen(String reason, long targetPos, long length)
      throws IOException {

    if (wrappedStream != null) {
      closeStream("reopen(" + reason + ")", contentRangeFinish, false);
    }

    contentRangeFinish = calculateRequestLimit(inputPolicy, targetPos,
        length, contentLength, readahead);
    LOG.debug("reopen({}) for {} range[{}-{}], length={},"
        + " streamPosition={}, nextReadPosition={}",
        uri, reason, targetPos, contentRangeFinish, length,  pos, nextReadPos);

    try {
      GetObjectRequest request = new GetObjectRequest(bucket, key)
          .withRange(targetPos, contentRangeFinish - 1);
      wrappedStream = client.getObject(request).getObjectContent();
      contentRangeStart = targetPos;
      if (wrappedStream == null) {
        throw new IOException("Null IO stream from reopen of (" + reason +  ") "
            + uri);
      }
    } catch (AmazonClientException e) {
      throw COSUtils.translateException("Reopen at position " + targetPos, uri, e);
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
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
          + " " + targetPos);
    }

    if (contentLength <= 0) {
      return;
    }

    // Lazy seek
    nextReadPos = targetPos;
  }

  /**
   * Seek without raising any exception. This is for use in
   * {@code finally} clauses
   * @param positiveTargetPos a target position which must be positive
   */
  private void seekQuietly(long positiveTargetPos) {
    try {
      seek(positiveTargetPos);
    } catch (IOException ioe) {
      LOG.debug("Ignoring IOE on seek of {} to {}",
          uri, positiveTargetPos, ioe);
    }
  }

  /**
   * Adjust the stream to a specific position.
   *
   * @param targetPos target seek position
   * @param length length of content that needs to be read from targetPos
   * @throws IOException
   */
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

      long forwardSeekLimit = Math.min(remainingInCurrentRequest,
          forwardSeekRange);
      boolean skipForward = remainingInCurrentRequest > 0
          && diff <= forwardSeekLimit;
      if (skipForward) {
        // the forward seek range is within the limits
        LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
        long skipped = wrappedStream.skip(diff);
        if (skipped > 0) {
          pos += skipped;
          // as these bytes have been read, they are included in the counter
          incrementBytesRead(diff);
        }

        if (pos == targetPos) {
          // all is well
          return;
        } else {
          // log a warning; continue to attempt to re-open
          LOG.warn("Failed to seek on {} to {}. Current position {}",
              uri, targetPos,  pos);
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
    closeStream("seekInStream()", contentRangeFinish, false);
    pos = targetPos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  /**
   * Perform lazy seek and adjust stream to correct position for reading.
   *
   * @param targetPos position from where data should be read
   * @param len length of the content that needs to be read
   */
  private void lazySeek(long targetPos, long len) throws IOException {
    //For lazy seek
    seekInStream(targetPos, len);

    //re-open at specific location if needed
    if (wrappedStream == null) {
      reopen("read from new offset", targetPos, len);
    }
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
      incrementBytesRead(1);
    }

    return byteRead;
  }

  /**
   * {@inheritDoc}
   *
   * This updates the statistics on read operations started and whether
   * or not the read operation "completed", that is: returned the exact
   * number of bytes requested.
   * @throws IOException if there are other problems
   */
  @Override
  public synchronized int read(byte[] buf, int off, int len)
      throws IOException {
    checkNotClosed();

    validatePositionedReadArgs(nextReadPos, buf, off, len);
    if (len == 0) {
      return 0;
    }

    if (contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, len);
    } catch (EOFException e) {
      // the end of the file has moved
      return -1;
    }

    int bytesRead;
    try {
      bytesRead = wrappedStream.read(buf, off, len);
    } catch (EOFException e) {
      onReadFailure(e, len);
      // the base implementation swallows EOFs.
      return -1;
    } catch (IOException e) {
      onReadFailure(e, len);
      bytesRead = wrappedStream.read(buf, off, len);
    }

    if (bytesRead > 0) {
      pos += bytesRead;
      nextReadPos += bytesRead;
    }
    incrementBytesRead(bytesRead);
    return bytesRead;
  }

  /**
   * Handle an IOE on a read by attempting to re-open the stream.
   * The filesystem's readException count will be incremented.
   * @param ioe exception caught
   * @param length length of data being attempted to read
   * @throws IOException any exception thrown on the re-open attempt
   */
  private void onReadFailure(IOException ioe, int length) throws IOException {
    LOG.info("Got exception while trying to read from stream {}"
        + " trying to recover: " + ioe, uri);
    LOG.debug("While trying to read from stream {}", uri, ioe);
    reopen("failure recovery", pos, length);
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the volatile {@link #closed} field.
   * @throws IOException if the connection is closed
   */
  private void checkNotClosed() throws IOException {
    if (closed) {
      throw new IOException(uri + ": " + FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  /**
   * Close the stream.
   * This triggers publishing of the stream statistics back to the filesystem
   * statistics.
   * This operation is synchronized, so that only one thread can attempt to
   * close the connection; all later/blocked calls are no-ops.
   * @throws IOException on any problem
   */
  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        // close or abort the stream
        closeStream("close() operation", contentRangeFinish, false);
        // this is actually a no-op
        super.close();
      } finally {
        LOG.trace("final of close for {}", uri);
      }
    }
  }

  /**
   * Close a stream: decide whether to abort or close, based on
   * the length of the stream and the current position.
   * If a close() is attempted and fails, the operation escalates to
   * an abort.
   *
   * This does not set the {@link #closed} flag.
   * @param reason reason for stream being closed; used in messages
   * @param length length of the stream
   * @param forceAbort force an abort; used if explicitly requested
   */
  private void closeStream(String reason, long length, boolean forceAbort) {
    if (wrappedStream != null) {

      // if the amount of data remaining in the current request is greater
      // than the readahead value: abort.
      long remaining = remainingInCurrentRequest();
      LOG.debug("Closing stream {}: {}", reason,
          forceAbort ? "abort" : "soft");
      boolean shouldAbort = forceAbort || remaining > readahead;
      if (!shouldAbort) {
        try {
          // clean close. This will read to the end of the stream,
          // so, while cleaner, can be pathological on a multi-GB object

          // explicitly drain the stream
          long drained = 0;
          while (wrappedStream.read() >= 0) {
            drained++;
          }
          LOG.debug("Drained stream of {} bytes", drained);

          // now close it
          wrappedStream.close();
          // this MUST come after the close, so that if the IO operations fail
          // and an abort is triggered, the initial attempt's statistics
          // aren't collected.
        } catch (IOException e) {
          // exception escalates to an abort
          LOG.debug("When closing {} stream for {}", uri, reason, e);
          shouldAbort = true;
        }
      }
      if (shouldAbort) {
        // Abort, rather than just close, the underlying stream.  Otherwise, the
        // remaining object payload is read from S3 while closing the stream.
        LOG.debug("Aborting stream");
        wrappedStream.abort();
      }
      LOG.debug("Stream {} {}: {}; remaining={} streamPos={},"
              + " nextReadPos={},"
              + " request range {}-{} length={}",
          uri, (shouldAbort ? "aborted" : "closed"), reason,
          remaining, pos, nextReadPos,
          contentRangeStart, contentRangeFinish,
          length);
      wrappedStream = null;
    }
  }

  /**
   * Forcibly reset the stream, by aborting the connection. The next
   * {@code read()} operation will trigger the opening of a new HTTPS
   * connection.
   *
   * This is potentially very inefficient, and should only be invoked
   * in extreme circumstances. It logs at info for this reason.
   * @return true if the connection was actually reset
   * @throws IOException if invoked on a closed stream
   */
  @InterfaceStability.Unstable
  public synchronized boolean resetConnection() throws IOException {
    checkNotClosed();
    boolean connectionOpen = wrappedStream != null;
    if (connectionOpen) {
      LOG.info("Forced reset of connection to {}", uri);
      closeStream("reset()", contentRangeFinish, true);
    }
    return connectionOpen;
  }

  @Override
  public synchronized int available() throws IOException {
    checkNotClosed();

    long remaining = remainingInFile();
    if (remaining > Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    }
    return (int)remaining;
  }

  /**
   * Bytes left in stream.
   * @return how many bytes are left to read
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public synchronized long remainingInFile() {
    return contentLength - pos;
  }

  /**
   * Bytes left in the current request.
   * Only valid if there is an active request.
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

  /**
   * String value includes statistics as well as stream state.
   * <b>Important: there are no guarantees as to the stability
   * of this value.</b>
   * @return a string value for printing in logs/diagnostics
   */
  @Override
  @InterfaceStability.Unstable
  public String toString() {
    synchronized (this) {
      final StringBuilder sb = new StringBuilder(
          "COSInputStream{");
      sb.append(uri);
      sb.append(" wrappedStream=")
          .append(wrappedStream != null ? "open" : "closed");
      sb.append(" read policy=").append(inputPolicy);
      sb.append(" pos=").append(pos);
      sb.append(" nextReadPos=").append(nextReadPos);
      sb.append(" contentLength=").append(contentLength);
      sb.append(" contentRangeStart=").append(contentRangeStart);
      sb.append(" contentRangeFinish=").append(contentRangeFinish);
      sb.append(" remainingInCurrentRequest=")
          .append(remainingInCurrentRequest());
      sb.append('\n');
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Subclass {@code readFully()} operation which only seeks at the start
   * of the series of operations; seeking back at the end.
   *
   * This is significantly higher performance if multiple read attempts are
   * needed to fetch the data, as it does not break the HTTP connection.
   *
   * To maintain thread safety requirements, this operation is synchronized
   * for the duration of the sequence.
   * {@inheritDoc}
   *
   */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(position, buffer, offset, length);
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
            throw new EOFException("EOF_IN_READ_FULLY");
          }
          nread += nbytes;
        }
      } finally {
        seekQuietly(oldPos);
      }
    }
  }

  @Override
  public synchronized void setReadahead(Long readaheadT) {
    if (readaheadT == null) {
      readahead = COSConstants.DEFAULT_READAHEAD_RANGE;
    } else {
      Preconditions.checkArgument(readahead >= 0, "Negative readahead value");
      readahead = readaheadT;
    }
  }

  /**
   * Get the current readahead value.
   * @return a non-negative readahead value
   */
  public synchronized long getReadahead() {
    return readahead;
  }

  /**
   * Calculate the limit for a get request, based on input policy
   * and state of object.
   * @param inputPolicy input policy
   * @param targetPos position of the read
   * @param length length of bytes requested; if less than zero "unknown"
   * @param contentLength total length of file
   * @param readahead current readahead value
   * @return the absolute value of the limit of the request
   */
  static long calculateRequestLimit(
      COSInputPolicy inputPolicy,
      long targetPos,
      long length,
      long contentLength,
      long readahead) {
    long rangeLimit;
    switch (inputPolicy) {
      case Random:
        // positioned.
        // read either this block, or the here + readahead value.
        rangeLimit = (length < 0) ? contentLength
            : targetPos + Math.max(readahead, length);
        break;

      case Sequential:
        // sequential: plan for reading the entire object.
        rangeLimit = contentLength;
        break;

      case Normal:
      default:
        rangeLimit = contentLength;
    }
    // cannot read past the end of the object
    rangeLimit = Math.min(contentLength, rangeLimit);
    return rangeLimit;
  }

  protected void validatePositionedReadArgs(long position,
      byte[] buffer, int offset, int length) throws EOFException {
    Preconditions.checkArgument(length >= 0, "length is negative");
    if (position < 0) {
      throw new EOFException("position is negative");
    }
    Preconditions.checkArgument(buffer != null, "Null buffer");
    if (buffer.length - offset < length) {
      throw new IndexOutOfBoundsException(
          "TOO_MANY_BYTES_FOR_DEST_BUFFER"
              + ": request length=" + length
              + ", with offset =" + offset
              + "; buffer capacity =" + (buffer.length - offset));
    }
  }

  /**
   * Increment the bytes read counter if there is a stats instance
   * and the number of bytes read is more than zero.
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(long bytesRead) {
    if (stats != null && bytesRead > 0) {
      stats.incrementBytesRead(bytesRead);
    }
  }

}
