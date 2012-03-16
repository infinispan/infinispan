/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.util.Util;

/**
 * Lucene's index segment files are chunked, for safe deletion of elements a read lock is
 * implemented so that all chunks are deleted only after the usage counter is decremented to zero.
 * FileReadLockKey is used as a key for the reference counters; a special purpose key was needed to
 * make atomic operation possible.
 * 
 * @author Sanne Grinovero
 * @since 4.0
 */
public final class FileReadLockKey implements Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 7789410500198851940L;

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
      if (!fileName.equals(other.fileName))
         return false;
      return indexName.equals(other.indexName);
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
