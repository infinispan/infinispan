package org.infinispan.lucene;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Used as a key to distinguish file chunk in cache.
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
@ProtoTypeId(ProtoStreamTypeIds.CHUNK_CACHE_KEY)
public final class ChunkCacheKey extends AbstractIndexScopedKey {

   private final int chunkId;

   private final String fileName;

   private final int bufferSize;

   @ProtoFactory
   public ChunkCacheKey(String indexName, String fileName, int chunkId, int bufferSize, int affinitySegmentId) {
      super(indexName, affinitySegmentId);
      if (fileName == null)
         throw new IllegalArgumentException("File name must not be null");
      this.fileName = fileName;
      this.chunkId = chunkId;
      this.bufferSize = bufferSize;
   }

   /**
    * Get the chunkId.
    *
    * @return the chunkId.
    */
   @ProtoField(number = 3, defaultValue = "0")
   public int getChunkId() {
      return chunkId;
   }

   /**
    * Get the file name.
    *
    * @return the file name.
    */
   @ProtoField(number = 4)
   public String getFileName() {
      return fileName;
   }

   /**
    * Get the bufferSize.
    *
    * @return the bufferSize.
    */
   @ProtoField(number = 5, defaultValue = "1024")
   public int getBufferSize() {
      return bufferSize;
   }

   @Override
   public <T> T accept(KeyVisitor<T> visitor) throws Exception {
      return visitor.visit(this);
   }

   @Override
   public int hashCode() {
      //bufferSize and indexName excluded from computation: not very relevant
      final int prime = 31;
      int result = prime + fileName.hashCode();
      return prime * result + chunkId; //adding chunkId as last is better
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null || ChunkCacheKey.class != obj.getClass())
         return false;
      ChunkCacheKey other = (ChunkCacheKey) obj;
      if (chunkId != other.chunkId)
         return false;
      if (!fileName.equals(other.fileName))
         return false;
      return indexName.equals(other.indexName);
   }

   /**
    * Changing the encoding could break backwards compatibility.
    *
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return "C|" + fileName + "|" + chunkId + "|" + bufferSize + "|" + indexName + "|" + affinitySegmentId;
   }
}
