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
 */
public class GridOutputStream extends OutputStream {
   
   final Cache<String, byte[]> cache;
   final int chunk_size;
   final String name;
   protected final GridFile file; // file representing this output stream
   int index = 0;                // index into the file for writing
   int local_index = 0;
   final byte[] current_buffer;
   static final Log log = LogFactory.getLog(GridOutputStream.class);


   GridOutputStream(GridFile file, boolean append, Cache<String, byte[]> cache, int chunk_size) {
      this.file = file;
      this.name = file.getPath();
      this.cache = cache;
      this.chunk_size = chunk_size;
      current_buffer = new byte[chunk_size];
   }

   public void write(int b) throws IOException {
      int remaining = getBytesRemainingInChunk();
      if (remaining == 0) {
         flush();
         local_index = 0;
         remaining = chunk_size;
      }
      current_buffer[local_index] = (byte) b;
      local_index++;
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
            local_index = 0;
            remaining = chunk_size;
         }
         int bytes_to_write = Math.min(remaining, len);
         System.arraycopy(b, off, current_buffer, local_index, bytes_to_write);
         off += bytes_to_write;
         len -= bytes_to_write;
         local_index += bytes_to_write;
         index += bytes_to_write;
      }
   }

   @Override
   public void close() throws IOException {
      flush();
      reset();
   }

   @Override
   public void flush() throws IOException {
      int chunk_number = getChunkNumber();
      String key = name + ".#" + chunk_number;
      byte[] val = new byte[local_index];
      System.arraycopy(current_buffer, 0, val, 0, local_index);
      cache.put(key, val);
      if (log.isTraceEnabled())
         log.trace("put(): index=" + index + ", key=" + key + ": " + val.length + " bytes");
      file.setLength(index);
   }

   private int getBytesRemainingInChunk() {
      return chunk_size - local_index;
   }


   private int getChunkNumber() {
      return (index - 1) / chunk_size;
   }

   private void reset() {
      index = local_index = 0;
   }
}
