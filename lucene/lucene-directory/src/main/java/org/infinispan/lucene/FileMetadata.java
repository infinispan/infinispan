package org.infinispan.lucene;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

/**
 * Header for Lucene files. Store only basic info about file. File data is divided into byte[]
 * chunks and stored under {@link org.infinispan.lucene.ChunkCacheKey}
 *
 * @since 4.0
 * @author Lukasz Moren
 * @see org.infinispan.lucene.FileCacheKey
 */
public final class FileMetadata {

   private long size = 0;
   private final int bufferSize;

   public FileMetadata(int bufferSize) {
      this.bufferSize = bufferSize;
   }

   private FileMetadata(long size, int bufferSize) {
      this.size = size;
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

   public static final class Externalizer extends AbstractExternalizer<FileMetadata> {

      @Override
      public void writeObject(ObjectOutput output, FileMetadata metadata) throws IOException {
         UnsignedNumeric.writeUnsignedLong(output, metadata.size);
         UnsignedNumeric.writeUnsignedInt(output, metadata.bufferSize);
      }

      @Override
      public FileMetadata readObject(ObjectInput input) throws IOException {
         long size = UnsignedNumeric.readUnsignedLong(input);
         int bufferSize = UnsignedNumeric.readUnsignedInt(input);
         return new FileMetadata(size, bufferSize);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.FILE_METADATA;
      }

      @Override
      public Set<Class<? extends FileMetadata>> getTypeClasses() {
         return Util.<Class<? extends FileMetadata>>asSet(FileMetadata.class);
      }

   }

}
