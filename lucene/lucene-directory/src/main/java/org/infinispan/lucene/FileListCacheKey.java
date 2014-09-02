package org.infinispan.lucene;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

/**
 * Cache key for a list with current files in cache
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class FileListCacheKey implements IndexScopedKey {

   private final String indexName;
   private final int hashCode;

   public FileListCacheKey(String indexName) {
      this.indexName = indexName;
      this.hashCode = generatedHashCode();
   }

   /**
    * Get the indexName.
    *
    * @return the indexName.
    */
   @Override
   public String getIndexName() {
      return indexName;
   }

   @Override
   public Object accept(KeyVisitor visitor) throws Exception {
      return visitor.visit(this);
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   private int generatedHashCode() {
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
      return "*|" + indexName;
   }

   public static final class Externalizer extends AbstractExternalizer<FileListCacheKey> {

      @Override
      public void writeObject(final ObjectOutput output, final FileListCacheKey key) throws IOException {
         output.writeUTF(key.indexName);
      }

      @Override
      public FileListCacheKey readObject(final ObjectInput input) throws IOException {
         String indexName = input.readUTF();
         return new FileListCacheKey(indexName);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_LIST_CACHE_KEY;
      }

      @Override
      public Set<Class<? extends FileListCacheKey>> getTypeClasses() {
         return Util.<Class<? extends FileListCacheKey>>asSet(FileListCacheKey.class);
      }

   }

}
