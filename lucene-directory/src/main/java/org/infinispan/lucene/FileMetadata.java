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

import java.io.Serializable;

/**
 * Header for Lucene files. Store only basic info about file. File data is divided into byte[]
 * chunks and stored under {@link org.infinispan.lucene.ChunkCacheKey}
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @see org.infinispan.lucene.FileCacheKey
 */
final class FileMetadata implements Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = -2605615719808221213L;
   
   private long lastModified;
   private long size = 0;

   public FileMetadata() {
      touch();
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

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || FileMetadata.class != o.getClass()) {
         return false;
      }
      FileMetadata metadata = (FileMetadata) o;
      return lastModified == metadata.lastModified && size == metadata.size;
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
   
}
