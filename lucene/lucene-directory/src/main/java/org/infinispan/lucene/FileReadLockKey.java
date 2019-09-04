package org.infinispan.lucene;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Lucene's index segment files are chunked, for safe deletion of elements a read lock is
 * implemented so that all chunks are deleted only after the usage counter is decremented to zero.
 * FileReadLockKey is used as a key for the reference counters; a special purpose key was needed to
 * make atomic operation possible.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.FILE_READ_LOCK_KEY)
public final class FileReadLockKey extends AbstractIndexScopedKey {

   private final String fileName;

   @ProtoFactory
   public FileReadLockKey(String indexName, String fileName, int affinitySegmentId) {
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
   @ProtoField(number = 3)
   public String getFileName() {
      return fileName;
   }

   @Override
   public <T> T accept(KeyVisitor<T> visitor) throws Exception {
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
      if (obj == null || FileReadLockKey.class != obj.getClass())
         return false;
      FileReadLockKey other = (FileReadLockKey) obj;
      return fileName.equals(other.fileName) && indexName.equals(other.indexName);
   }

   @Override
   public String toString() {
      return "RL|" + fileName + "|" + indexName + "|" + affinitySegmentId;
   }
}
