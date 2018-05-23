/*
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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.ibm.stocator.fs.cos.COSDataBlocks.DataBlock.DestState.Writing;
import static com.ibm.stocator.fs.cos.COSDataBlocks.DataBlock.DestState.Upload;
import static com.ibm.stocator.fs.cos.COSDataBlocks.DataBlock.DestState.Closed;
import static com.ibm.stocator.fs.common.Utils.closeAll;

/**
 * Set of classes to support output streaming into blocks which are then
 * uploaded as to COS as a single PUT, or as part of a multipart request.
 */
final class COSDataBlocks {

  private static final Logger LOG = LoggerFactory.getLogger(COSDataBlocks.class);

  private COSDataBlocks() {
  }

  /**
   * @param b byte array containing data
   * @param off offset in array where to start
   * @param len number of bytes to be written
   * @throws NullPointerException for a null buffer
   * @throws IndexOutOfBoundsException if indices are out of range
   */
  static void validateWriteArgs(byte[] b, int off, int len)
      throws IOException {
    Preconditions.checkNotNull(b);
    if ((off < 0) || (off > b.length) || (len < 0)
        || ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException(
          "write (b[" + b.length + "], " + off + ", " + len + ')');
    }
  }

  /**
   * Create a factory.
   * @param owner factory owner
   * @param name factory name -the option from {@link Constants}
   * @return the factory, ready to be initialized
   * @throws IllegalArgumentException if the name is unknown
   */
  static BlockFactory createFactory(COSAPIClient owner,
      String name) {
    switch (name) {
      case COSConstants.FAST_UPLOAD_BUFFER_ARRAY:
        return new ArrayBlockFactory(owner);
      case COSConstants.FAST_UPLOAD_BUFFER_DISK:
        return new DiskBlockFactory(owner);
      default:
        throw new IllegalArgumentException("Unsupported block buffer"
            + " \"" + name + '"');
    }
  }

  /**
   * The output information for an upload.
   * It can be one of a file or an input stream.
   * When closed, any stream is closed. Any source file is untouched.
   */
  static final class BlockUploadData implements Closeable {
    private final File file;
    private final InputStream uploadStream;

    /**
     * File constructor; input stream will be null
     * @param fileT file to upload
     */
    BlockUploadData(File fileT) {
      Preconditions.checkArgument(fileT.exists(), "No file: " + fileT);
      file = fileT;
      uploadStream = null;
    }

    /**
     * Stream constructor, file field will be null
     * @param uploadStreamT stream to upload
     */
    BlockUploadData(InputStream uploadStreamT) {
      Preconditions.checkNotNull(uploadStreamT, "rawUploadStream");
      uploadStream = uploadStreamT;
      file = null;
    }

    /**
     * Predicate: does this instance contain a file reference
     * @return true if there is a file
     */
    boolean hasFile() {
      return file != null;
    }

    /**
     * Get the file, if there is one.
     * @return the file for uploading, or null
     */
    File getFile() {
      return file;
    }

    /**
     * Get the raw upload stream, if the object was
     * created with one.
     * @return the upload stream or null
     */
    InputStream getUploadStream() {
      return uploadStream;
    }

    /**
     * Close: closes any upload stream provided in the constructor.
     * @throws IOException inherited exception
     */
    @Override
    public void close() throws IOException {
      closeAll(LOG, uploadStream);
    }
  }

  // ====================================================================

  /**
   * Use byte arrays on the heap for storage.
   */
  static class ArrayBlockFactory extends BlockFactory {

    ArrayBlockFactory(COSAPIClient owner) {
      super(owner);
    }

    @Override
    DataBlock create(String key, long index, int limit)
        throws IOException {
      return new ByteArrayBlock(0, limit);
    }

  }

  static class COSByteArrayOutputStream extends ByteArrayOutputStream {

    COSByteArrayOutputStream(int size) {
      super(size);
    }

    /**
     * InputStream backed by the internal byte array
     *
     * @return
     */
    ByteArrayInputStream getInputStream() {
      ByteArrayInputStream bin = new ByteArrayInputStream(buf, 0, count);
      reset();
      buf = null;
      return bin;
    }
  }

  /**
   * Stream to memory via a {@code ByteArrayOutputStream}.
   *
   * This was taken from {@code S3AFastOutputStream} and has the
   * same problem which surfaced there: it can consume a lot of heap space
   * proportional to the mismatch between writes to the stream and
   * the JVM-wide upload bandwidth to the S3 endpoint.
   * The memory consumption can be limited by tuning the filesystem settings
   * to restrict the number of queued/active uploads.
   */

  static class ByteArrayBlock extends DataBlock {
    private COSByteArrayOutputStream buffer;
    private final int limit;
    // cache data size so that it is consistent after the buffer is reset.
    private Integer dataSize;

    ByteArrayBlock(long index,
        int limitT) {
      super(index);
      limit = limitT;
      buffer = new COSByteArrayOutputStream(limit);
      blockAllocated();
    }

    /**
     * Get the amount of data; if there is no buffer then the size is 0.
     * @return the amount of data available to upload
     */
    @Override
    int dataSize() {
      return dataSize != null ? dataSize : buffer.size();
    }

    @Override
    BlockUploadData startUpload() throws IOException {
      super.startUpload();
      dataSize = buffer.size();
      ByteArrayInputStream bufferData = buffer.getInputStream();
      buffer = null;
      return new BlockUploadData(bufferData);
    }

    @Override
    boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - dataSize();
    }

    @Override
    int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      buffer.write(b, offset, written);
      return written;
    }

    @Override
    protected void innerClose() {
      buffer = null;
      blockReleased();
    }

    @Override
    public String toString() {
      return "ByteArrayBlock{"
          + "index=" + index
          + ", state=" + getState()
          + ", limit=" + limit
          + ", dataSize=" + dataSize + '}';
    }
  }

  /**
   * Buffer blocks to disk.
   */
  static class DiskBlockFactory extends BlockFactory {

    DiskBlockFactory(COSAPIClient owner) {
      super(owner);
    }

    /**
     * Create a temp file and a {@link DiskBlock} instance to manage it.
     *
     * @param key = a string containing the partition number, task number,
     *              attempt number, and timestamp
     * @param index block index
     * @param limit limit of the block
     * @return the new block
     * @throws IOException IO problems
     */
    @Override
    DataBlock create(String key, long index, int limit) throws IOException {
      String tmpPrefix = (key.replaceAll("/", "-")).replaceAll(":", "-");
      File destFile = getOwner()
          .createTmpFileForWrite(String.format("cosblock-%04d-" + tmpPrefix, index));
      return new DiskBlock(destFile, limit, index);
    }
  }

  /**
   * Stream to a file.
   * This will stop at the limit; the caller is expected to create a new block.
   */
  static class DiskBlock extends DataBlock {

    private int bytesWritten;
    private final File bufferFile;
    private final int limit;
    private BufferedOutputStream out;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    DiskBlock(File bufferFileT, int limitT, long index) throws FileNotFoundException {
      super(index);
      limit = limitT;
      bufferFile = bufferFileT;
      blockAllocated();
      out = new BufferedOutputStream(new FileOutputStream(bufferFile));
    }

    @Override
    int dataSize() {
      return bytesWritten;
    }

    @Override
    boolean hasCapacity(long bytes) {
      return dataSize() + bytes <= limit;
    }

    @Override
    int remainingCapacity() {
      return limit - bytesWritten;
    }

    @Override
    int write(byte[] b, int offset, int len) throws IOException {
      super.write(b, offset, len);
      int written = Math.min(remainingCapacity(), len);
      out.write(b, offset, written);
      bytesWritten += written;
      return written;
    }

    @Override
    BlockUploadData startUpload() throws IOException {
      super.startUpload();
      try {
        out.flush();
      } finally {
        out.close();
        out = null;
      }
      return new BlockUploadData(bufferFile);
    }

    /**
     * The close operation will delete the destination file if it still
     * exists.
     * @throws IOException IO problems
     */
    @SuppressWarnings("UnnecessaryDefault")
    @Override
    protected void innerClose() throws IOException {
      final DestState state = getState();
      LOG.debug("Closing {}", this);
      switch (state) {
        case Writing:
          if (bufferFile.exists()) {
            // file was not uploaded
            LOG.debug("Block[{}]: Deleting buffer file as upload did not start",
                index);
            closeBlock();
          }
          break;

        case Upload:
          LOG.debug("Block[{}]: Buffer file {} exists close upload stream",
              index, bufferFile);
          break;

        case Closed:
          closeBlock();
          break;

        default:
      }
    }

    /**
     * Flush operation will flush to disk.
     * @throws IOException IOE raised on FileOutputStream
     */
    @Override
    void flush() throws IOException {
      super.flush();
      out.flush();
    }

    @Override
    public String toString() {
      String sb = "FileBlock{"
          + "index=" + index
          + ", destFile="
          + bufferFile
          + ", state=" + getState()
          + ", dataSize=" + dataSize()
          + ", limit=" + limit
          + '}';
      return sb;
    }

    /**
     * Close the block.
     * This will delete the block's buffer file if the block has
     * not previously been closed.
     */
    void closeBlock() {
      LOG.debug("block[{}]: closeBlock()", index);
      if (!closed.getAndSet(true)) {
        blockReleased();
        if (!bufferFile.delete() && bufferFile.exists()) {
          LOG.warn("delete({}) returned false",
              bufferFile.getAbsoluteFile());
        }
      } else {
        LOG.debug("block[{}]: skipping re-entrant closeBlock()", index);
      }
    }
  }

  /**
   * Base class for block factories.
   */
  abstract static class BlockFactory implements Closeable {

    private final COSAPIClient owner;

    protected BlockFactory(COSAPIClient ownerT) {
      owner = ownerT;
    }

    /**
     * Owner.
     */
    protected COSAPIClient getOwner() {
      return owner;
    }

    /**
     * Implement any close/cleanup operation.
     * Base class is a no-op
     * @throws IOException Inherited exception; implementations should
     * avoid raising it
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Create a block.
     *
     * @param index index of block
     * @param limit limit of the block
     * @param statistics stats to work with
     * @return a new block
     */
    abstract DataBlock create(String key, long index, int limit)
        throws IOException;

  }

  /**
   * This represents a block being uploaded.
   */
  abstract static class DataBlock implements Closeable {

    enum DestState {Writing, Upload, Closed}

    private volatile DestState state = Writing;
    protected final long index;

    protected DataBlock(long indexT) {
      index = indexT;
    }

    /**
     * Atomically enter a state, verifying current state.
     * @param current current state. null means "no check"
     * @param next next state
     * @throws IllegalStateException if the current state is not as expected
     */
    protected final synchronized void enterState(DestState current,
        DestState next) throws IllegalStateException {
      verifyState(current);
      LOG.debug("{}: entering state {}", this, next);
      state = next;
    }

    /**
     * Verify that the block is in the declared state
     * @param expected expected state
     * @throws IllegalStateException if the DataBlock is in the wrong state
     */
    protected final void verifyState(DestState expected)
        throws IllegalStateException {
      if (expected != null && state != expected) {
        throw new IllegalStateException("Expected stream state " + expected
            + " -but actual state is " + state + " in " + this);
      }
    }

    /**
     * Current state.
     * @return the current state
     */
    final DestState getState() {
      return state;
    }

    /**
     * Return the current data size.
     * @return the size of the data
     */
    abstract int dataSize();

    /**
     * Predicate to verify that the block has the capacity to write
     * the given set of bytes
     * @param bytes number of bytes desired to be written
     * @return true if there is enough space
     */
    abstract boolean hasCapacity(long bytes);

    /**
     * Predicate to check if there is data in the block.
     * @return true if there is
     */
    boolean hasData() {
      return dataSize() > 0;
    }

    /**
     * The remaining capacity in the block before it is full.
     * @return the number of bytes remaining
     */
    abstract int remainingCapacity();

    /**
     * Write a series of bytes from the buffer, from the offset.
     * Returns the number of bytes written.
     * Only valid in the state {@code Writing}.
     * Base class verifies the state but does no writing
     * @param buffer buffer
     * @param offset offset
     * @param length length of write
     * @return number of bytes written
     * @throws IOException trouble
     */
    int write(byte[] buffer, int offset, int length) throws IOException {
      verifyState(Writing);
      Preconditions.checkArgument(buffer != null, "Null buffer");
      Preconditions.checkArgument(length >= 0, "length is negative");
      Preconditions.checkArgument(offset >= 0, "offset is negative");
      Preconditions.checkArgument(
          !(buffer.length - offset < length),
          "buffer shorter than amount of data to write");
      return 0;
    }

    /**
     * Flush the output.
     * Only valid in the state {@code Writing}.
     * In the base class, this is a no-op
     * @throws IOException any IO problem
     */
    void flush() throws IOException {
      verifyState(Writing);
    }

    /**
     * Switch to the upload state and return a stream for uploading.
     * Base class calls {@link #enterState(DestState, DestState)} to
     * manage the state machine.
     * @return the stream
     * @throws IOException trouble
     */
    BlockUploadData startUpload() throws IOException {
      LOG.debug("Start datablock[{}] upload", index);
      enterState(Writing, Upload);
      return null;
    }

    /**
     * Enter the closed state.
     * @return true if the class was in any other state, implying that
     * the subclass should do its close operations
     */
    protected synchronized boolean enterClosedState() {
      if (!state.equals(Closed)) {
        enterState(null, Closed);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void close() throws IOException {
      if (enterClosedState()) {
        LOG.debug("Closed {}", this);
        innerClose();
      }
    }

    /**
     * Inner close logic for subclasses to implement.
     */
    protected void innerClose() throws IOException {

    }

    /**
     * A block has been allocated.
     */
    protected void blockAllocated() {
    }

    /**
     * A block has been released.
     */
    protected void blockReleased() {
    }
  }

}
