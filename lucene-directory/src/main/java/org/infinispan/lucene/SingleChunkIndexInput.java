/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;

/**
 * SingleChunkIndexInput can be used instead of InfinispanIndexInput to read a segment
 * when it has a size small enough to fit in a single chunk.
 * In this quite common case for some segment types we
 * don't need the readLock to span multiple chunks, the pointer to the buffer is safe enough.
 * This leads to an extreme simple implementation.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public class SingleChunkIndexInput extends IndexInput {

   private final byte[] buffer;
   private int bufferPosition;

   public SingleChunkIndexInput(AdvancedCache<CacheKey, Object> cache, FileCacheKey fileKey, FileMetadata fileMetadata) throws FileNotFoundException {
      CacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), 0);
      buffer = (byte[]) cache.withFlags(Flag.SKIP_LOCKING).get(key);
      if (buffer == null) {
         throw new FileNotFoundException("Chunk value could not be found for key " + key);
      }
      bufferPosition = 0;
   }

   @Override
   public void close() throws IOException {
      //nothing to do
   }

   @Override
   public long getFilePointer() {
      return bufferPosition;
   }

   @Override
   public long length() {
      return buffer.length;
   }

   @Override
   public byte readByte() throws IOException {
      return buffer[bufferPosition++];
   }

   @Override
   public void readBytes(byte[] b, int offset, int len) throws IOException {
      System.arraycopy(buffer, bufferPosition, b, offset, len);
      bufferPosition+=len;
   }

   @Override
   public void seek(long pos) throws IOException {
      //Lucene might use positions larger than length(), in
      //this case you have to position the pointer to eof.
      bufferPosition = (int) Math.min(pos, buffer.length);
   }

}
