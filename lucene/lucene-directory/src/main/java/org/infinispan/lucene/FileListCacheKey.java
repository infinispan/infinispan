package org.infinispan.lucene;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Cache key for a list with current files in cache.
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
@ProtoTypeId(ProtoStreamTypeIds.FILE_LIST_CACHE_KEY)
public final class FileListCacheKey extends AbstractIndexScopedKey {

   @ProtoFactory
   public FileListCacheKey(String indexName, int affinitySegmentId) {
      super(indexName, affinitySegmentId);
   }

   @Override
   public <T> T accept(KeyVisitor<T> visitor) throws Exception {
      return visitor.visit(this);
   }

   @Override
   public int hashCode() {
      return 31 + indexName.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null || FileListCacheKey.class != obj.getClass())
         return false;
      FileListCacheKey other = (FileListCacheKey) obj;
      return indexName.equals(other.indexName);
   }

   /**
    * Changing the encoding could break backwards compatibility .
    *
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return "*|" + indexName + "|" + affinitySegmentId;
   }
}
