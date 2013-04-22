/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.util.Util;

/**
 * Used as a key to distinguish file chunk in cache.
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class ChunkCacheKey implements Serializable, IndexScopedKey {

   /** The serialVersionUID */
   private static final long serialVersionUID = 4429712073623290126L;

   private final int chunkId;
   private final String indexName;
   private final String fileName;
   private final int bufferSize;
   private final int hashCode;

   public ChunkCacheKey(final String indexName, final String fileName, final int chunkId, final int bufferSize) {
      if (fileName == null)
         throw new IllegalArgumentException("filename must not be null");
      this.indexName = indexName;
      this.fileName = fileName;
      this.chunkId = chunkId;
      this.bufferSize = bufferSize;
      this.hashCode = generatedHashCode();
   }

   /**
    * Get the chunkId.
    * 
    * @return the chunkId.
    */
   public int getChunkId() {
      return chunkId;
   }

   /**
    * Get the bufferSize.
    * 
    * @return the bufferSize.
    */
   public int getBufferSize() {
      return bufferSize;
   }

   /**
    * Get the indexName.
    * 
    * @return the indexName.
    */
   public String getIndexName() {
      return indexName;
   }

   @Override
   public Object accept(KeyVisitor visitor) throws Exception {
      return visitor.visit(this);
   }

   /**
    * Get the fileName.
    * 
    * @return the fileName.
    */
   public String getFileName() {
      return fileName;
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   private int generatedHashCode() {
      //bufferSize and indexName excluded from computation: not very relevant
      final int prime = 31;
      int result = prime + fileName.hashCode();
      return prime * result + chunkId; //adding chunkId as last is better
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (ChunkCacheKey.class != obj.getClass())
         return false;
      ChunkCacheKey other = (ChunkCacheKey) obj;
      if (chunkId != other.chunkId)
         return false;
      if (!fileName.equals(other.fileName))
         return false;
      if (bufferSize != other.bufferSize)
         return false;
      return indexName.equals(other.indexName);
   }

   /**
    * Changing the encoding could break backwards compatibility
    * 
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return fileName + "|" + chunkId + "|" + bufferSize + "|" + indexName;
   }

   public static final class Externalizer extends AbstractExternalizer<ChunkCacheKey> {

      @Override
      public void writeObject(final ObjectOutput output, final ChunkCacheKey key) throws IOException {
         output.writeUTF(key.indexName);
         output.writeUTF(key.fileName);
         UnsignedNumeric.writeUnsignedInt(output, key.chunkId);
         UnsignedNumeric.writeUnsignedInt(output, key.bufferSize);
      }

      @Override
      public ChunkCacheKey readObject(final ObjectInput input) throws IOException {
         final String indexName = input.readUTF();
         final String fileName = input.readUTF();
         final int chunkId = UnsignedNumeric.readUnsignedInt(input);
         final int bufferSize = UnsignedNumeric.readUnsignedInt(input);
         return new ChunkCacheKey(indexName, fileName, chunkId, bufferSize);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CHUNK_CACHE_KEY;
      }

      @Override
      public Set<Class<? extends ChunkCacheKey>> getTypeClasses() {
         return Util.<Class<? extends ChunkCacheKey>>asSet(ChunkCacheKey.class);
      }

   }

}
