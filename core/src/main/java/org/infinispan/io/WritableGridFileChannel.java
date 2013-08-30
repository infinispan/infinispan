package org.infinispan.io;

import org.infinispan.Cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

/**
 * @author Marko Luksa
 */
public class WritableGridFileChannel implements WritableByteChannel {

   private int position;
   private int localIndex;
   private byte[] currentBuffer;

   private boolean closed;

   private final FileChunkMapper fileChunkMapper;
   private final int chunkSize; // Guaranteed to be a power of 2
   private GridFile file;

   WritableGridFileChannel(GridFile file, Cache<String, byte[]> cache, boolean append) {
      fileChunkMapper = new FileChunkMapper(file, cache);
      chunkSize = fileChunkMapper.getChunkSize();
      this.file = file;

      if (append)
         initForAppending();
      else
         initForOverwriting();
   }

   private void initForOverwriting() {
      this.currentBuffer = createEmptyChunk();
      this.position = 0;
      this.localIndex = 0;
   }

   private void initForAppending() {
      this.currentBuffer = lastChunkIsFull() ? createEmptyChunk() : fetchLastChunk();
      this.position = (int) file.length();
      this.localIndex = ModularArithmetic.mod(position, chunkSize);
   }

   private byte[] createEmptyChunk() {
      return new byte[chunkSize];
   }

   private byte[] fetchLastChunk() {
      byte[] chunk = fileChunkMapper.fetchChunk(getLastChunkNumber());
      return createFullSizeCopy(chunk);
   }

   private int getLastChunkNumber() {
      return getChunkNumber((int) file.length() - 1);
   }

   private byte[] createFullSizeCopy(byte[] val) {
      byte chunk[] = createEmptyChunk();
      if (val != null) {
         System.arraycopy(val, 0, chunk, 0, val.length);
      }
      return chunk;
   }

   private boolean lastChunkIsFull() {
      return ModularArithmetic.mod(file.length(), chunkSize) == 0;
   }

   @Override
   public int write(ByteBuffer src) throws IOException {
      checkOpen();

      int bytesWritten = 0;
      while (src.remaining() > 0) {
         int bytesWrittenToChunk = writeToChunk(src);
         bytesWritten += bytesWrittenToChunk;
      }
      return bytesWritten;
   }

   private int writeToChunk(ByteBuffer src) throws IOException {
      int remainingInChunk = getBytesRemainingInChunk();
      if (remainingInChunk == 0) {
         flush();
         localIndex = 0;
         remainingInChunk = chunkSize;
      }

      int bytesToWrite = Math.min(remainingInChunk, src.remaining());
      src.get(currentBuffer, localIndex, bytesToWrite);
      localIndex += bytesToWrite;
      position += bytesToWrite;
      return bytesToWrite;
   }

   private int getBytesRemainingInChunk() {
      return currentBuffer.length - localIndex;
   }

   public void flush() throws IOException {
      storeChunkInCache();
      updateFileLength();
   }

   private void updateFileLength() {
      file.setLength(position);
   }

   private void storeChunkInCache() {
      fileChunkMapper.storeChunk(getChunkNumberOfPreviousByte(), currentBuffer, localIndex);
   }

   private int getChunkNumberOfPreviousByte() {
      return getChunkNumber(position - 1);
   }

   private int getChunkNumber(int position) {
      return position / chunkSize;
   }

   @Override
   public boolean isOpen() {
      return !closed;
   }

   @Override
   public void close() throws IOException {
      flush();
      position = localIndex = 0;
      closed = true;
   }

   private void checkOpen() throws ClosedChannelException {
      if (!isOpen()) {
         throw new ClosedChannelException();
      }
   }
}
