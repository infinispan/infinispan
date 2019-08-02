package org.infinispan.lucene;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * Header for Lucene files. Store only basic info about file. File data is divided into byte[]
 * chunks and stored under {@link org.infinispan.lucene.ChunkCacheKey}
 *
 * @since 4.0
 * @author Lukasz Moren
 * @see org.infinispan.lucene.FileCacheKey
 */
public final class FileMetadata {

   @ProtoField(number = 1, defaultValue = "0")
   int bufferSize;

   @ProtoField(number = 2, defaultValue = "0")
   long size = 0;

   FileMetadata() {}

   public FileMetadata(int bufferSize) {
      this.bufferSize = bufferSize;
   }

   public long getSize() {
      return size;
   }

   public void setSize(long size) {
      this.size = size;
   }

   public int getBufferSize() {
      return bufferSize;
   }

   public int getNumberOfChunks() {
      if (size % bufferSize == 0) {
         return (int) size / bufferSize;
      }
      else {
         return (int) (size / bufferSize) + 1;
      }
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
      return  size == metadata.size && bufferSize == metadata.bufferSize;
   }

   @Override
   public int hashCode() {
      return (int) (size ^ (size >>> 32));
   }

   @Override
   public String toString() {
      return "FileMetadata{" +  " size=" + size + '}';
   }
}
