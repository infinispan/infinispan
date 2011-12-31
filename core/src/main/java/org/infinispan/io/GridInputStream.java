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
   
   final Cache<String, byte[]> cache;
   final int chunk_size;
   final String name;
   protected final GridFile file; // file representing this input stream
   int index = 0;                // index into the file for writing
   int local_index = 0;
   byte[] current_buffer = null;
   boolean end_reached = false;

   GridInputStream(GridFile file, Cache<String, byte[]> cache) {
      this.file = file;
      this.name = file.getPath();
      this.cache = cache;
      this.chunk_size = file.getChunkSize();
   }

   public int read() throws IOException {
      int bytes_remaining_to_read = getBytesRemainingInChunk();
      if (bytes_remaining_to_read == 0) {
         if (end_reached)
            return -1;
         current_buffer = fetchNextChunk();
         local_index = 0;
         if (current_buffer == null)
            return -1;
         else if (current_buffer.length < chunk_size)
            end_reached = true;
         bytes_remaining_to_read = getBytesRemainingInChunk();
      }
      int retval = current_buffer[local_index++];
      index++;
      return retval;
   }

   @Override
   public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      int bytes_read = 0;
      while (len > 0) {
         int bytes_remaining_to_read = getBytesRemainingInChunk();
         if (bytes_remaining_to_read == 0) {
            if (end_reached)
               return bytes_read > 0 ? bytes_read : -1;
            current_buffer = fetchNextChunk();
            local_index = 0;
            if (current_buffer == null)
               return bytes_read > 0 ? bytes_read : -1;
            else if (current_buffer.length < chunk_size)
               end_reached = true;
            bytes_remaining_to_read = getBytesRemainingInChunk();
         }
         int bytes_to_read = Math.min(len, bytes_remaining_to_read);
         // bytes_to_read=Math.min(bytes_to_read, current_buffer.length - local_index);
         System.arraycopy(current_buffer, local_index, b, off, bytes_to_read);
         local_index += bytes_to_read;
         off += bytes_to_read;
         len -= bytes_to_read;
         bytes_read += bytes_to_read;
         index += bytes_to_read;
      }

      return bytes_read;
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
      local_index = index = 0;
      end_reached = false;
   }

   private int getBytesRemainingInChunk() {
      // return chunk_size - local_index;
      return current_buffer == null ? 0 : current_buffer.length - local_index;
   }

   private byte[] fetchNextChunk() {
      int chunk_number = getChunkNumber();
      String key = name + ".#" + chunk_number;
      byte[] val = cache.get(key);
      if (log.isTraceEnabled())
         log.trace("fetching index=" + index + ", key=" + key + ": " + (val != null ? val.length + " bytes" : "null"));
      return val;
   }

   private int getChunkNumber() {
      return index / chunk_size;
   }
}
