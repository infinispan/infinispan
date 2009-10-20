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
 * Cache key for a list with current files in cache
 * 
 * @since 4.0
 * @author Lukasz Moren
 * @author Sanne Grinovero
 */
final class FileListCacheKey implements Serializable, CacheKey {

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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
      return result;
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
      if (indexName == null) {
         if (other.indexName != null)
            return false;
      } else if (!indexName.equals(other.indexName))
         return false;
      return true;
   }
   
   @Override
   public String toString() {
      return "FileListCacheKey{indexName='" + indexName + '}';
   }
   
}
