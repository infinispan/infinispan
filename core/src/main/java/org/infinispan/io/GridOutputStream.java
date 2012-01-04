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
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Bela Ban
 * @author Marko Luksa
 */
public class GridOutputStream extends OutputStream {
   
   private final Cache<String, byte[]> cache;
   private final int chunkSize;
   private final String name;
   protected final GridFile file; // file representing this output stream
   private int index;                     // index into the file for writing
   private int localIndex;
   private final byte[] currentBuffer;
   private static final Log log = LogFactory.getLog(GridOutputStream.class);
   private int numberOfChunksWhenOpened;

   GridOutputStream(GridFile file, boolean append, Cache<String, byte[]> cache) {
      this.file = file;
      this.name = file.getPath();
      this.cache = cache;
      this.chunkSize = file.getChunkSize();

      index = append ? (int) file.length() : 0;
      localIndex = index % chunkSize;
      currentBuffer = append && !isLastChunkFull() ? fetchLastChunk() : new byte[chunkSize];

      numberOfChunksWhenOpened = getLastChunkNumber() + 1;
   }

   private boolean isLastChunkFull() {
      long bytesRemainingInLastChunk = file.length() % chunkSize;
      return bytesRemainingInLastChunk == 0;
   }

   private byte[] fetchLastChunk() {
      String key = getChunkKey(getLastChunkNumber());
      byte[] val = cache.get(key);

      byte chunk[] = new byte[chunkSize];
      if (val != null) {
         System.arraycopy(val, 0, chunk, 0, val.length);
      }
      return chunk;
   }

   private int getLastChunkNumber() {
      return getChunkNumber((int) file.length() - 1);
   }

   public void write(int b) throws IOException {
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         flush();
         localIndex = 0;
         remaining = chunkSize;
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
         int remaining = getBytesRemainingInChunk();
         if (remaining == 0) {
            flush();
            localIndex = 0;
            remaining = chunkSize;
         }
         int bytesToWrite = Math.min(remaining, len);
         System.arraycopy(b, off, currentBuffer, localIndex, bytesToWrite);
         off += bytesToWrite;
         len -= bytesToWrite;
         localIndex += bytesToWrite;
         index += bytesToWrite;
      }
   }

   @Override
   public void close() throws IOException {
      flush();
      removeExcessChunks();
      reset();
   }

   private void removeExcessChunks() {
      for (int i = getLastChunkNumber()+1; i<numberOfChunksWhenOpened; i++) {
         removeChunk(i);
      }
   }

   private void removeChunk(int chunkNumber) {
      cache.remove(getChunkKey(chunkNumber));
   }

   @Override
   public void flush() throws IOException {
      String key = getChunkKey(getChunkNumberOfPreviousByte());
      byte[] val = new byte[localIndex];
      System.arraycopy(currentBuffer, 0, val, 0, localIndex);
      cache.put(key, val);
      if (log.isTraceEnabled())
         log.trace("put(): index=" + index + ", key=" + key + ": " + val.length + " bytes");
      file.setLength(index);
   }

   private String getChunkKey(int chunkNumber) {
      return name + ".#" + chunkNumber;
   }

   private int getBytesRemainingInChunk() {
      return chunkSize - localIndex;
   }

   private int getChunkNumberOfPreviousByte() {
      return getChunkNumber(index - 1);
   }

   private int getChunkNumber(int position) {
      return position / chunkSize;
   }

   private void reset() {
      index = localIndex = 0;
   }
}
