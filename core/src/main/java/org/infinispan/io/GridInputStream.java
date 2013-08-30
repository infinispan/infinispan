package org.infinispan.io;

import org.infinispan.Cache;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Bela Ban
 * @author Marko Luksa
 * @author Manik Surtani
 */
public class GridInputStream extends InputStream {

   private int index = 0;                // index into the file for writing
   private int localIndex = 0;
   private byte[] currentBuffer = null;
   private int fSize;
   private boolean streamClosed = false;
   private final FileChunkMapper fileChunkMapper;
   private final int chunkSize; // Guaranteed to be a power of 2

   GridInputStream(GridFile file, Cache<String, byte[]> cache) {
      fileChunkMapper = new FileChunkMapper(file, cache);
      chunkSize = fileChunkMapper.getChunkSize();
      fSize = (int)file.length();
   }

   @Override public int read() throws IOException {
      assertOpen();
      if (isEndReached())
         return -1;
      if (getBytesRemainingInChunk() == 0)
         getChunk();
      int retval = 0x0ff & currentBuffer[localIndex++];
      index++;
      return retval;
   }

   @Override
   public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] bytes, int offset, int length) throws IOException {
      assertOpen();
      int totalBytesRead = 0;
      while (length > 0) {
         int bytesRead = readFromChunk(bytes, offset, length);
         if (bytesRead == -1)
            return totalBytesRead > 0 ? totalBytesRead : -1;
         offset += bytesRead;
         length -= bytesRead;
         totalBytesRead += bytesRead;
      }

      return totalBytesRead;
   }

   private int readFromChunk(byte[] b, int off, int len) {
      if (isEndReached())
         return -1;
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         getChunk();
         remaining = getBytesRemainingInChunk();
      }
      int bytesToRead = Math.min(len, remaining);
      System.arraycopy(currentBuffer, localIndex, b, off, bytesToRead);
      localIndex += bytesToRead;
      index += bytesToRead;
      return bytesToRead;
   }

   @Override public long skip(long length) throws IOException {
      assertOpen();
      if (length <= 0)
         return 0;

      int bytesToSkip = Math.min((int)length, getBytesRemainingInStream());
      index += bytesToSkip;
      if (bytesToSkip <= getBytesRemainingInChunk()) {
         localIndex += bytesToSkip;
      } else {
         getChunk();
         localIndex = ModularArithmetic.mod(index, chunkSize);
      }
      return bytesToSkip;
   }

   @Override
   public int available() throws IOException {
      assertOpen();
      return getBytesRemainingInChunk();  // Return bytes in chunk
   }

   @Override
   public void close() throws IOException {
      localIndex = index = 0;
      streamClosed = true;
   }

   private boolean isEndReached() {
      return index == fSize;
   }

   private void assertOpen() throws IOException{
       if (streamClosed) throw new IOException("Stream is closed");
   }

   private int getBytesRemainingInChunk() {
      return currentBuffer == null ? 0 : currentBuffer.length - localIndex;
   }

   private int getBytesRemainingInStream() {
      return fSize - index;
   }

   private void getChunk() {
      currentBuffer = fileChunkMapper.fetchChunk(getChunkNumber());
      localIndex = 0;
   }

   private int getChunkNumber() {
      return index / chunkSize;
   }
}
