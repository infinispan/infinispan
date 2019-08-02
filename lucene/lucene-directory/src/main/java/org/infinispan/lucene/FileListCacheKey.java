package org.infinispan.lucene;

/**
 * Cache key for a list with current files in cache
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class FileListCacheKey extends AbstractIndexScopedKey {

   FileListCacheKey() {};

   public FileListCacheKey(String indexName, final int affinitySegmentId) {
      super(indexName, affinitySegmentId);
   }

   @Override
   public Object accept(KeyVisitor visitor) throws Exception {
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
      if (obj == null)
         return false;
      if (FileListCacheKey.class != obj.getClass())
         return false;
      FileListCacheKey other = (FileListCacheKey) obj;
      return indexName.equals(other.indexName);
   }

   /**
    * Changing the encoding could break backwards compatibility
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return "*|" + indexName + "|" + affinitySegmentId;
   }
}
