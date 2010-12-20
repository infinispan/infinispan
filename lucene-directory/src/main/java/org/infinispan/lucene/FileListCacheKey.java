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

import org.infinispan.marshall.Marshalls;

/**
 * Cache key for a list with current files in cache
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
public final class FileListCacheKey implements Serializable {

   /** The serialVersionUID */
   private static final long serialVersionUID = 8965108175527988255L;
   
   private final String indexName;
   private final int hashCode;

   public FileListCacheKey(String indexName) {
      this.indexName = indexName;
      this.hashCode = generatedHashCode();
   }

   /**
    * Get the indexName.
    * 
    * @return the indexName.
    */
   public String getIndexName() {
      return indexName;
   }
   
   @Override
   public int hashCode() {
      return hashCode;
   }

   private int generatedHashCode() {
      return 31 + indexName.hashCode();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (FileListCacheKey.class != obj.getClass())
         return false;
      FileListCacheKey other = (FileListCacheKey) obj;
      return indexName.equals(other.indexName);
   }
   
   /**
    * Changing the encoding could break backwards compatibility
    * @see LuceneKey2StringMapper#getKeyMapping(String)
    */
   @Override
   public String toString() {
      return "*|" + indexName;
   }
   
   @Marshalls(typeClasses = FileListCacheKey.class, id = ExternalizerIds.FILE_LIST_CACHE_KEY)
   public static class Externalizer implements org.infinispan.marshall.Externalizer<FileListCacheKey> {

      @Override
      public void writeObject(ObjectOutput output, FileListCacheKey key) throws IOException {
         output.writeUTF(key.indexName);
      }

      @Override
      public FileListCacheKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String indexName = input.readUTF();
         return new FileListCacheKey(indexName);
      }

   }
   
}
