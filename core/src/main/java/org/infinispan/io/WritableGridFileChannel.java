/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

   private FileChunkMapper fileChunkMapper;
   private GridFile file;

   WritableGridFileChannel(GridFile file, Cache<String, byte[]> cache, boolean append) {
      fileChunkMapper = new FileChunkMapper(file, cache);
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
      this.localIndex = position % getChunkSize();
   }

   private byte[] createEmptyChunk() {
      return new byte[getChunkSize()];
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
      return file.length() % getChunkSize() == 0;
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
         remainingInChunk = getChunkSize();
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
      return position / getChunkSize();
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

   private int getChunkSize() {
      return fileChunkMapper.getChunkSize();
   }
}
