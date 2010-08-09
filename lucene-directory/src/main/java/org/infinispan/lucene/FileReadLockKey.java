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
 * Lucene's index segment files are chunked, for safe deletion of elements a read lock is
 * implemented so that all chunks are deleted only after the usage counter is decremented to zero.
 * FileReadLockKey is used as a key for the reference counters; a special purpose key was needed to
 * make atomic operation possible.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
final class FileReadLockKey implements Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 7789410500198851940L;

   private final String indexName;
   private final String fileName;
   private final int hashCode;

   FileReadLockKey(String indexName, String fileName) {
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
   public int hashCode() {
      return hashCode;
   }

   private int generateHashCode() {
      final int prime = 31;
      int result = prime + fileName.hashCode();
      result = prime * result + indexName.hashCode();
      return result;
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
      if (!fileName.equals(other.fileName))
         return false;
      if (!indexName.equals(other.indexName))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "FileReadLockKey{fileName=" + fileName + ", indexName=" + indexName + "} ";
   }

}