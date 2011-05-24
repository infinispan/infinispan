/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
import java.util.Set;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.util.Util;

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
   
   private long lastModified = 0;
   private long size = 0;
   private final int bufferSize;

   public FileMetadata(int bufferSize) {
      this.bufferSize = bufferSize;
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
   
   public static final class Externalizer extends AbstractExternalizer<FileMetadata> {

      @Override
      public void writeObject(ObjectOutput output, FileMetadata metadata) throws IOException {
         UnsignedNumeric.writeUnsignedLong(output, metadata.lastModified);
         UnsignedNumeric.writeUnsignedLong(output, metadata.size);
         UnsignedNumeric.writeUnsignedInt(output, metadata.bufferSize);
      }

      @Override
      public FileMetadata readObject(ObjectInput input) throws IOException {
         long lastModified = UnsignedNumeric.readUnsignedLong(input);
         long size = UnsignedNumeric.readUnsignedLong(input);
         int bufferSize = UnsignedNumeric.readUnsignedInt(input);
         return new FileMetadata(lastModified, size, bufferSize);
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
