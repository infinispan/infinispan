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
import java.io.InputStream;

/**
 * @author Bela Ban
 */
public class GridInputStream extends InputStream {

   private static final Log log = LogFactory.getLog(GridInputStream.class);
   
   private final Cache<String, byte[]> cache;
   private final int chunkSize;
   private final String name;
   protected final GridFile file; // file representing this input stream
   private int index = 0;                // index into the file for writing
   private int localIndex = 0;
   private byte[] currentBuffer = null;
   private boolean endReached = false;

   GridInputStream(GridFile file, Cache<String, byte[]> cache) {
      this.file = file;
      this.name = file.getPath();
      this.cache = cache;
      this.chunkSize = file.getChunkSize();
   }

   public int read() throws IOException {
      int bytesRemainingToRead = getBytesRemainingInChunk();
      if (bytesRemainingToRead == 0) {
         if (endReached)
            return -1;
         currentBuffer = fetchNextChunk();
         localIndex = 0;
         if (currentBuffer == null)
            return -1;
         else if (currentBuffer.length < chunkSize)
            endReached = true;
      }
      int retval = currentBuffer[localIndex++];
      index++;
      return retval;
   }

   @Override
   public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      int bytesRead = 0;
      while (len > 0) {
         int bytesRemainingToRead = getBytesRemainingInChunk();
         if (bytesRemainingToRead == 0) {
            if (endReached)
               return bytesRead > 0 ? bytesRead : -1;
            currentBuffer = fetchNextChunk();
            localIndex = 0;
            if (currentBuffer == null)
               return bytesRead > 0 ? bytesRead : -1;
            else if (currentBuffer.length < chunkSize)
               endReached = true;
            bytesRemainingToRead = getBytesRemainingInChunk();
         }
         int bytesToRead = Math.min(len, bytesRemainingToRead);
         // bytesToRead=Math.min(bytesToRead, currentBuffer.length - localIndex);
         System.arraycopy(currentBuffer, localIndex, b, off, bytesToRead);
         localIndex += bytesToRead;
         off += bytesToRead;
         len -= bytesToRead;
         bytesRead += bytesToRead;
         index += bytesToRead;
      }

      return bytesRead;
   }

   @Override
   public long skip(long n) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public int available() throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public void close() throws IOException {
      localIndex = index = 0;
      endReached = false;
   }

   private int getBytesRemainingInChunk() {
      // return chunkSize - localIndex;
      return currentBuffer == null ? 0 : currentBuffer.length - localIndex;
   }

   private byte[] fetchNextChunk() {
      String key = getChunkKey(getChunkNumber());
      byte[] val = cache.get(key);
      if (log.isTraceEnabled())
         log.trace("fetching index=" + index + ", key=" + key + ": " + (val != null ? val.length + " bytes" : "null"));
      return val;
   }

   private String getChunkKey(int chunkNumber) {
      return name + ".#" + chunkNumber;
   }

   private int getChunkNumber() {
      return index / chunkSize;
   }
}
