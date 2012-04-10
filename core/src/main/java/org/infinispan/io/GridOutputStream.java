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
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridOutputStream extends OutputStream {

   private int index;                     // index into the file for writing
   private int localIndex;
   private final byte[] currentBuffer;
   private int numberOfChunksWhenOpened;

   private FileChunkMapper fileChunkMapper;
   private GridFile file;

   GridOutputStream(GridFile file, boolean append, Cache<String, byte[]> cache) {
      fileChunkMapper = new FileChunkMapper(file, cache);
      this.file = file;

      index = append ? (int) file.length() : 0;
      localIndex = index % getChunkSize();
      currentBuffer = append && !isLastChunkFull() ? fetchLastChunk() : createEmptyChunk();

      numberOfChunksWhenOpened = getLastChunkNumber() + 1;
   }

   private byte[] createEmptyChunk() {
      return new byte[getChunkSize()];
   }

   private boolean isLastChunkFull() {
      long bytesRemainingInLastChunk = file.length() % getChunkSize();
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
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         flush();
         localIndex = 0;
      }
      currentBuffer[localIndex] = (byte) b;
      localIndex++;
      index++;
   }

   @Override
   public void write(byte[] b) throws IOException {
      if (b != null)
         write(b, 0, b.length);
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
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
         remaining = getChunkSize();
      }
      int bytesToWrite = Math.min(remaining, len);
      System.arraycopy(b, off, currentBuffer, localIndex, bytesToWrite);
      localIndex += bytesToWrite;
      index += bytesToWrite;
      return bytesToWrite;
   }

   @Override
   public void close() throws IOException {
      flush();
      removeExcessChunks();
      reset();
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
      return getChunkSize() - localIndex;
   }

   private int getChunkNumber(int position) {
      return position / getChunkSize();
   }

   private void reset() {
      index = localIndex = 0;
   }

   private int getChunkSize() {
      return fileChunkMapper.getChunkSize();
   }

}
