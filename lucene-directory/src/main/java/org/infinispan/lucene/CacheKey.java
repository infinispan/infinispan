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
 * Abstract class used as a key for Infinispan cache to distinct its values. Connection of fields:
 * indexName and fileName is unique, even if we share one cache between all DirectoryProvider's
 * 
 * @author Lukasz Moren
 * @since 4.0
 * @see org.hibernate.search.store.infinispan.InfinispanDirectory#cache
 */
public abstract class CacheKey implements Serializable {

   protected final String indexName;
   protected final String fileName;

   protected int hashCode;

   protected CacheKey(String indexName, String fileName) {
      this.indexName = indexName;
      this.fileName = fileName;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      CacheKey that = (CacheKey) o;

      if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null) {
         return false;
      }
      if (indexName != null ? !indexName.equals(that.indexName) : that.indexName != null) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      if (hashCode == 0) {
         hashCode = indexName != null ? indexName.hashCode() : 0;
         hashCode = 31 * hashCode + (fileName != null ? fileName.hashCode() : 0);
         hashCode = 31 * hashCode + (this.getClass().getName() != null ? this.getClass().getName().hashCode() : 0);
      }
      return hashCode;
   }

   @Override
   public String toString() {
      return "CacheKey{" + "fileName='" + fileName + '\'' + ", indexName='" + indexName + '\'' + '}';
   }

   public final String getIndexName() {
      return indexName;
   }

   public final String getFileName() {
      return fileName;
   }

}
