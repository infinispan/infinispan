/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.io;

import org.infinispan.Cache;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridInputStream extends InputStream {

   private int index = 0;                // index into the file for writing
   private int localIndex = 0;
   private byte[] currentBuffer = null;
   private int fileSize;
   private boolean closed = false;
   private FileChunkMapper fileChunkMapper;

   GridInputStream(GridFile file, Cache<String, byte[]> cache) {
      fileChunkMapper = new FileChunkMapper(file, cache);
      fileSize = (int)file.length();
   }

   @Override
   public int read() throws IOException {
      checkClosed();
      if (isEndReached())
         return -1;
      if (getBytesRemainingInChunk() == 0)
         fetchChunk();
      int retval = 0x0ff&currentBuffer[localIndex++];
      index++;
      return retval;
   }

   @Override
   public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      checkClosed();
      int totalBytesRead = 0;
      while (len > 0) {
         int bytesRead = readFromChunk(b, off, len);
         if (bytesRead == -1)
            return totalBytesRead > 0 ? totalBytesRead : -1;
         off += bytesRead;
         len -= bytesRead;
         totalBytesRead += bytesRead;
      }

      return totalBytesRead;
   }

   private int readFromChunk(byte[] b, int off, int len) {
      if (isEndReached())
         return -1;
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         fetchChunk();
         remaining = getBytesRemainingInChunk();
      }
      int bytesToRead = Math.min(len, remaining);
      System.arraycopy(currentBuffer, localIndex, b, off, bytesToRead);
      localIndex += bytesToRead;
      index += bytesToRead;
      return bytesToRead;
   }

   @Override
   public long skip(long len) throws IOException {
      checkClosed();
      if (len <= 0)
         return 0;

      int bytesToSkip = Math.min((int)len, getBytesRemainingInStream());
      index += bytesToSkip;
      if (bytesToSkip <= getBytesRemainingInChunk()) {
         localIndex += bytesToSkip;
      } else {
         fetchChunk();
         localIndex = index % getChunkSize();
      }
      return bytesToSkip;
   }

   @Override
   public int available() throws IOException {
      checkClosed();
      return getBytesRemainingInChunk();
   }

   @Override
   public void close() throws IOException {
      localIndex = index = 0;
      closed = true;
   }

   private boolean isEndReached() {
      return index == fileSize;
   }

   private void checkClosed() throws IOException{
       if(closed){
           throw new IOException("Stream is closed");
       }
   }

   private int getBytesRemainingInChunk() {
      return currentBuffer == null ? 0 : currentBuffer.length - localIndex;
   }

   private int getBytesRemainingInStream() {
      return fileSize - index;
   }

   private void fetchChunk() {
      currentBuffer = fileChunkMapper.fetchChunk(getChunkNumber());
      localIndex = 0;
   }

   private int getChunkNumber() {
      return index / getChunkSize();
   }

   private int getChunkSize() {
      return fileChunkMapper.getChunkSize();
   }
}
