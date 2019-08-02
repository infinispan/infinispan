package org.infinispan.lucene;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * Lucene's index segment files are chunked, for safe deletion of elements a read lock is
 * implemented so that all chunks are deleted only after the usage counter is decremented to zero.
 * FileReadLockKey is used as a key for the reference counters; a special purpose key was needed to
 * make atomic operation possible.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
public final class FileReadLockKey extends AbstractIndexScopedKey {

   @ProtoField(number = 3)
   String fileName;

   FileReadLockKey() {}

   public FileReadLockKey(final String indexName, final String fileName, final int affinitySegmentId) {
      super(indexName, affinitySegmentId);
      if (indexName == null)
         throw new IllegalArgumentException("indexName shall not be null");
      if (fileName == null)
         throw new IllegalArgumentException("fileName shall not be null");
      this.fileName = fileName;
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
      return "RL|" + fileName + "|"+ indexName + "|" + affinitySegmentId;
   }
}
