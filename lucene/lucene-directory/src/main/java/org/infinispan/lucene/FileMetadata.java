package org.infinispan.lucene;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Header for Lucene files. Store only basic info about file. File data is divided into byte[]
 * chunks and stored under {@link org.infinispan.lucene.ChunkCacheKey}
 *
 * @since 4.0
 * @author Lukasz Moren
 * @see org.infinispan.lucene.FileCacheKey
 */
@ProtoTypeId(ProtoStreamTypeIds.FILE_METADATA)
public final class FileMetadata {

   private final int bufferSize;

   private long size;

   @ProtoFactory
   public FileMetadata(int bufferSize, long size) {
      this.bufferSize = bufferSize;
      this.size = size;
   }

   public FileMetadata(int bufferSize) {
      this(bufferSize, 0);
   }

   @ProtoField(number = 1, defaultValue = "1024")
   public int getBufferSize() {
      return bufferSize;
   }

   @ProtoField(number = 2, defaultValue = "0")
   public long getSize() {
      return size;
   }

   public void setSize(long size) {
      this.size = size;
   }

   public int getNumberOfChunks() {
      int numChunks = (int) size / bufferSize;
      return size % bufferSize == 0 ? numChunks : numChunks + 1;
   }

   public boolean isMultiChunked() {
      return size > bufferSize;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || FileMetadata.class != o.getClass()) {
         return false;
      }
      FileMetadata metadata = (FileMetadata) o;
      return size == metadata.size && bufferSize == metadata.bufferSize;
   }

   @Override
   public int hashCode() {
      return (int) (size ^ (size >>> 32));
   }

   @Override
   public String toString() {
      return "FileMetadata{size=" + size + '}';
   }
}
