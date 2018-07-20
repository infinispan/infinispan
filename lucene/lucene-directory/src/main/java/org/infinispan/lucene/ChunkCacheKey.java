package org.infinispan.lucene;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * Used as a key to distinguish file chunk in cache.
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class ChunkCacheKey extends AbstractIndexScopedKey {

   @ProtoField(number = 3, defaultValue = "0")
   int chunkId;

   @ProtoField(number = 4, name = "file")
   String fileName;

   @ProtoField(number = 5, defaultValue = "0")
   int bufferSize;

   ChunkCacheKey() {}

   public ChunkCacheKey(final String indexName, final String fileName, final int chunkId, final int bufferSize, final int affinitySegmentId) {
      super(indexName, affinitySegmentId);
      if (fileName == null)
         throw new IllegalArgumentException("filename must not be null");
      this.fileName = fileName;
      this.chunkId = chunkId;
      this.bufferSize = bufferSize;
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
    * Get the fileName.
    *
    * @return the fileName.
    */
   public String getFileName() {
      return fileName;
   }

   @Override
   public Object accept(KeyVisitor visitor) throws Exception {
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
      if (obj == null)
         return false;
      if (ChunkCacheKey.class != obj.getClass())
         return false;
      ChunkCacheKey other = (ChunkCacheKey) obj;
      if (chunkId != other.chunkId)
         return false;
      if (!fileName.equals(other.fileName))
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
      return "C|" + fileName + "|" + chunkId + "|" + bufferSize + "|" + indexName + "|" + affinitySegmentId;
   }
}
