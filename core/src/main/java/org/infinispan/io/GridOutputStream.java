package org.infinispan.io;

import org.infinispan.Cache;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @author Marko Luksa
 * @author Manik Surtani
 */
public class GridOutputStream extends OutputStream {

   private int index;                     // index into the file for writing
   private int localIndex;
   private final byte[] currentBuffer;
   private int numberOfChunksWhenOpened;

   private final FileChunkMapper fileChunkMapper;
   private final int chunkSize; // Guaranteed to be a power of 2
   private GridFile file;
   private boolean streamClosed;

   GridOutputStream(GridFile file, boolean append, Cache<String, byte[]> cache) {
      fileChunkMapper = new FileChunkMapper(file, cache);
      chunkSize = fileChunkMapper.getChunkSize();
      this.file = file;

      index = append ? (int) file.length() : 0;
      localIndex = ModularArithmetic.mod(index, chunkSize);
      currentBuffer = append && !isLastChunkFull() ? fetchLastChunk() : createEmptyChunk();

      numberOfChunksWhenOpened = getLastChunkNumber() + 1;
   }

   private byte[] createEmptyChunk() {
      return new byte[chunkSize];
   }

   private boolean isLastChunkFull() {
      long bytesRemainingInLastChunk = ModularArithmetic.mod(file.length(), chunkSize);
      return bytesRemainingInLastChunk == 0;
   }

   private byte[] fetchLastChunk() {
      byte[] chunk = fileChunkMapper.fetchChunk(getLastChunkNumber());
      return createFullSizeCopy(chunk);
   }

   private byte[] createFullSizeCopy(byte[] val) {
      byte chunk[] = createEmptyChunk();
      if (val != null) {
         System.arraycopy(val, 0, chunk, 0, val.length);
      }
      return chunk;
   }

   private int getLastChunkNumber() {
      return getChunkNumber((int) file.length() - 1);
   }

   @Override
   public void write(int b) throws IOException {
      assertOpen();
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         flush();
         localIndex = 0;
      }
      currentBuffer[localIndex] = (byte) b;
      localIndex++;
      index++;
   }

   private void assertOpen() throws IOException {
      if (streamClosed) throw new IOException("Stream is closed");
    }

   @Override
   public void write(byte[] b) throws IOException {
      assertOpen();
      if (b != null)
         write(b, 0, b.length);
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      assertOpen();
      while (len > 0) {
         int bytesWritten = writeToChunk(b, off, len);
         off += bytesWritten;
         len -= bytesWritten;
      }
   }

   private int writeToChunk(byte[] b, int off, int len) throws IOException {
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         flush();
         localIndex = 0;
         remaining = chunkSize;
      }
      int bytesToWrite = Math.min(remaining, len);
      System.arraycopy(b, off, currentBuffer, localIndex, bytesToWrite);
      localIndex += bytesToWrite;
      index += bytesToWrite;
      return bytesToWrite;
   }

   @Override
   public void close() throws IOException {
      if (streamClosed) return;
      flush();
      removeExcessChunks();
      reset();
      streamClosed = true;
   }

   private void removeExcessChunks() {
      for (int i = getLastChunkNumber()+1; i<numberOfChunksWhenOpened; i++) {
         fileChunkMapper.removeChunk(i);
      }
   }

   @Override
   public void flush() throws IOException {
      storeChunk();
      file.setLength(index);
   }

   private void storeChunk() {
      fileChunkMapper.storeChunk(getChunkNumber(index - 1), currentBuffer, localIndex);
   }

   private int getBytesRemainingInChunk() {
      return chunkSize - localIndex;
   }

   private int getChunkNumber(int position) {
      return position / chunkSize;
   }

   private void reset() {
      index = localIndex = 0;
   }
}
