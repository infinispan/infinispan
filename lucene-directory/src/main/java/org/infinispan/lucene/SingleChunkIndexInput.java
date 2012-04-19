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
package org.infinispan.lucene;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.infinispan.AdvancedCache;

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
@SuppressWarnings("unchecked")
final public class SingleChunkIndexInput extends IndexInput {

   private final byte[] buffer;
   private int bufferPosition;

   public SingleChunkIndexInput(final AdvancedCache<?, ?> chunksCache, final FileCacheKey fileKey, final FileMetadata fileMetadata) {
      super(fileKey.getFileName());
      ChunkCacheKey key = new ChunkCacheKey(fileKey.getIndexName(), fileKey.getFileName(), 0);
      byte[] b = (byte[]) chunksCache.get(key);
      if (b == null) {
         buffer = new byte[0];
      }
      else {
         buffer = b;
      }
      bufferPosition = 0;
   }

   @Override
   public void close() {
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
      if (bufferPosition >= buffer.length) {
         throw new IOException("Read past EOF");
      }
      return buffer[bufferPosition++];
   }

   @Override
   public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
      if (buffer.length - bufferPosition < len) {
         throw new IOException("Read past EOF");
      }
      System.arraycopy(buffer, bufferPosition, b, offset, len);
      bufferPosition+=len;
   }

   @Override
   public void seek(final long pos) {
      //Lucene might use positions larger than length(), in
      //this case you have to position the pointer to eof.
      bufferPosition = (int) Math.min(pos, buffer.length);
   }

}
