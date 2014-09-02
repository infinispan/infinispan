package org.infinispan.lucene;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

/**
 * Used as a key for file headers in a cache
 *
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class FileCacheKey implements IndexScopedKey {

   private final String indexName;
   private final String fileName;
   private final int hashCode;

   public FileCacheKey(final String indexName, final String fileName) {
      if (fileName == null)
         throw new IllegalArgumentException("filename must not be null");
      this.indexName = indexName;
      this.fileName = fileName;
      this.hashCode = generatedHashCode();
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
      return fileName + "|M|"+ indexName;
   }

   public static final class Externalizer extends AbstractExternalizer<FileCacheKey> {

      @Override
      public void writeObject(final ObjectOutput output, final FileCacheKey key) throws IOException {
         output.writeUTF(key.indexName);
         output.writeUTF(key.fileName);
      }

      @Override
      public FileCacheKey readObject(final ObjectInput input) throws IOException {
         String indexName = input.readUTF();
         String fileName = input.readUTF();
         return new FileCacheKey(indexName, fileName);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_CACHE_KEY;
      }

      @Override
      public Set<Class<? extends FileCacheKey>> getTypeClasses() {
         return Util.<Class<? extends FileCacheKey>>asSet(FileCacheKey.class);
      }
   }

}
