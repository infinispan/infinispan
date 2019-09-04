package org.infinispan.lucene;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Used as a key for file headers in a cache
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
@ProtoTypeId(ProtoStreamTypeIds.FILE_CACHE_KEY)
public final class FileCacheKey extends AbstractIndexScopedKey {

   private final String fileName;

   @ProtoFactory
   public FileCacheKey(String indexName, String fileName, int affinitySegmentId) {
      super(indexName, affinitySegmentId);
      if (fileName == null)
         throw new IllegalArgumentException("filename must not be null");
      this.fileName = fileName;
   }

   @Override
   public <T> T accept(KeyVisitor<T> visitor) throws Exception {
      return visitor.visit(this);
   }

   /**
    * Get the file name.
    *
    * @return the file name.
    */
   @ProtoField(number = 3)
   public String getFileName() {
      return fileName;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = prime + fileName.hashCode();
      return prime * result + indexName.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null || FileCacheKey.class != obj.getClass())
         return false;
      FileCacheKey other = (FileCacheKey) obj;
      return fileName.equals(other.fileName) && indexName.equals(other.indexName);
   }

   /**
    * Changing the encoding could break backwards compatibility.
    *
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return "M|" + fileName + "|" + indexName + "|" + affinitySegmentId;
   }
}
