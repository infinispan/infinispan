/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.lucene;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.Marshalls;

/**
 * Header for Lucene files. Store only basic info about file. File data is divided into byte[]
 * chunks and stored under {@link org.infinispan.lucene.ChunkCacheKey}
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @see org.infinispan.lucene.FileCacheKey
 */
public final class FileMetadata implements Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = -7150923427362644166L;
   
   private long lastModified;
   private long size = 0;
   private int bufferSize;

   public FileMetadata() {
      touch();
   }

   private FileMetadata(long lastModified, long size, int bufferSize) {
      this.lastModified = lastModified;
      this.size = size;
      this.bufferSize = bufferSize;
   }

   public void touch() {
      setLastModified(System.currentTimeMillis());
   }

   public long getLastModified() {
      return lastModified;
   }

   public void setLastModified(long lastModified) {
      this.lastModified = lastModified;
   }

   public long getSize() {
      return size;
   }

   public void setSize(long size) {
      this.size = size;
   }

   public void setBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
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

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || FileMetadata.class != o.getClass()) {
         return false;
      }
      FileMetadata metadata = (FileMetadata) o;
      return lastModified == metadata.lastModified && size == metadata.size && bufferSize == metadata.bufferSize;
   }

   @Override
   public int hashCode() {
      int result = (int) (lastModified ^ (lastModified >>> 32));
      result = 31 * result + (int) (size ^ (size >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "FileMetadata{" + "lastModified=" + lastModified + ", size=" + size + '}';
   }
   
   @Marshalls(typeClasses = FileMetadata.class, id = ExternalizerIds.FILE_METADATA)
   public static class Externalizer implements org.infinispan.marshall.Externalizer<FileMetadata> {

      @Override
      public void writeObject(ObjectOutput output, FileMetadata metadata) throws IOException {
         UnsignedNumeric.writeUnsignedLong(output, metadata.lastModified);
         UnsignedNumeric.writeUnsignedLong(output, metadata.size);
         UnsignedNumeric.writeUnsignedInt(output, metadata.bufferSize);
      }

      @Override
      public FileMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         long lastModified = UnsignedNumeric.readUnsignedLong(input);
         long size = UnsignedNumeric.readUnsignedLong(input);
         int bufferSize = UnsignedNumeric.readUnsignedInt(input);
         return new FileMetadata(lastModified, size, bufferSize);
      }

   }
   
}
