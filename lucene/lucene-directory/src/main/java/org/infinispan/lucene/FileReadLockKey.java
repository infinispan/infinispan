package org.infinispan.lucene;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

/**
 * Lucene's index segment files are chunked, for safe deletion of elements a read lock is
 * implemented so that all chunks are deleted only after the usage counter is decremented to zero.
 * FileReadLockKey is used as a key for the reference counters; a special purpose key was needed to
 * make atomic operation possible.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public final class FileReadLockKey implements IndexScopedKey {

   private final String indexName;
   private final String fileName;
   private final int hashCode;

   public FileReadLockKey(final String indexName, final String fileName) {
      if (indexName == null)
         throw new IllegalArgumentException("indexName shall not be null");
      if (fileName == null)
         throw new IllegalArgumentException("fileName shall not be null");
      this.indexName = indexName;
      this.fileName = fileName;
      this.hashCode = generateHashCode();
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
      return hashCode;
   }

   private int generateHashCode() {
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
      if (FileReadLockKey.class != obj.getClass())
         return false;
      FileReadLockKey other = (FileReadLockKey) obj;
      return fileName.equals(other.fileName) && indexName.equals(other.indexName);
   }

   @Override
   public String toString() {
      return fileName + "|RL|"+ indexName;
   }

   public static final class Externalizer extends AbstractExternalizer<FileReadLockKey> {

      @Override
      public void writeObject(final ObjectOutput output, final FileReadLockKey key) throws IOException {
         output.writeUTF(key.indexName);
         output.writeUTF(key.fileName);
      }

      @Override
      public FileReadLockKey readObject(final ObjectInput input) throws IOException {
         String indexName = input.readUTF();
         String fileName = input.readUTF();
         return new FileReadLockKey(indexName, fileName);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_READLOCK_KEY;
      }

      @Override
      public Set<Class<? extends FileReadLockKey>> getTypeClasses() {
         return Util.<Class<? extends FileReadLockKey>>asSet(FileReadLockKey.class);
      }

   }

}
