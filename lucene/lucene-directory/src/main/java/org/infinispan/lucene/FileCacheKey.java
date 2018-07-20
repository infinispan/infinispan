package org.infinispan.lucene;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * Used as a key for file headers in a cache
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class FileCacheKey extends AbstractIndexScopedKey {

   @ProtoField(number = 3, name = "file")
   String fileName;

   FileCacheKey() {}

   public FileCacheKey(final String indexName, final String fileName, final int affinitySegmentId) {
      super(indexName, affinitySegmentId);
      if (fileName == null)
         throw new IllegalArgumentException("filename must not be null");
      this.fileName = fileName;
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
      final int prime = 31;
      int result = prime + fileName.hashCode();
      return prime * result + indexName.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (FileCacheKey.class != obj.getClass())
         return false;
      FileCacheKey other = (FileCacheKey) obj;
      if (!fileName.equals(other.fileName))
         return false;
      return indexName.equals(other.indexName);
   }

   /**
    * Changing the encoding could break backwards compatibility
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return "M|" + fileName + "|"+ indexName + "|" + affinitySegmentId;
   }
}
