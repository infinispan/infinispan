package org.infinispan.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

import org.infinispan.Cache;

/**
 * @author Marko Luksa
 */
public class ReadableGridFileChannel implements ReadableByteChannel {

   private int position = 0;
   private int localIndex = 0;
   private byte[] currentBuffer;

   private boolean closed;

   private final FileChunkMapper fileChunkMapper;
   private final int chunkSize; // Guaranteed to be a power of 2

   private long fileLength;

   ReadableGridFileChannel(GridFile file, Cache<String, byte[]> cache) {
      fileChunkMapper = new FileChunkMapper(file, cache);
      chunkSize = fileChunkMapper.getChunkSize();
      fileLength = (int) file.length();
   }

   @Override
   public int read(ByteBuffer dst) throws IOException {
      long tbr = getTotalBytesRemaining();
      if (tbr == 0) {
         return -1;
      }

      int bytesRead = 0;
      long len = Math.min(dst.remaining(), tbr);
      while (len > 0) {
         int bytesReadFromChunk = readFromChunk(dst, len);
         len -= bytesReadFromChunk;
         bytesRead += bytesReadFromChunk;
      }
      return bytesRead;
   }

   private int readFromChunk(ByteBuffer dst, long len) {
      int bytesRemaining = getBytesRemainingInChunk();
      if (bytesRemaining == 0) {
         fetchNextChunk();
         bytesRemaining = getBytesRemainingInChunk();
      }
      int bytesToRead = Math.min((int)len, bytesRemaining);
      dst.put(currentBuffer, localIndex, bytesToRead);

      position += bytesToRead;
      localIndex += bytesToRead;
      return bytesToRead;
   }

   private void fetchNextChunk() {
      int chunkNumber = getChunkNumber(position);
      currentBuffer = fileChunkMapper.fetchChunk(chunkNumber);
      localIndex = 0;
   }

   private long getTotalBytesRemaining() {
      return fileLength - position;
   }

   @Override
   public boolean isOpen() {
      return !closed;
   }

   @Override
   public void close() throws IOException {
      reset();
      closed = true;
   }

   public long position() throws IOException {
      checkOpen();
      return position;
   }

   public void position(long newPosition) throws IOException {
      if (newPosition < 0) {
         throw new IllegalArgumentException("newPosition may not be negative");
      }
      checkOpen();

      int newPos = (int) newPosition;
      int chunkNumberOfNewPosition = getChunkNumber(newPos);
      if (getChunkNumber(position - 1) != chunkNumberOfNewPosition) {
         currentBuffer = fileChunkMapper.fetchChunk(chunkNumberOfNewPosition);
      }
      position = newPos;
      localIndex = ModularArithmetic.mod(newPos, chunkSize);
   }

   private void checkOpen() throws ClosedChannelException {
      if (!isOpen()) {
         throw new ClosedChannelException();
      }
   }

   public long size() throws IOException {
      return fileLength;
   }

   private int getChunkNumber(int position) {
      return position < 0 ? -1 : (position / chunkSize);
   }

   private void reset() {
      position = localIndex = 0;
   }

   private int getBytesRemainingInChunk() {
      return currentBuffer == null ? 0 : currentBuffer.length - localIndex;
   }

}
