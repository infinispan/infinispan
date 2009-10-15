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
 * Used as a key for file headers in a cache
 * 
 * @author Lukasz Moren
 */
public class FileCacheKey extends CacheKey implements Serializable {

   protected final boolean isLockKey;

   public FileCacheKey(String indexName, String fileName) {
      this(indexName, fileName, false);
   }

   public FileCacheKey(String indexName, String fileName, boolean isLockKey) {
      super(indexName, fileName);
      this.isLockKey = isLockKey;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      if (!super.equals(o)) {
         return false;
      }

      FileCacheKey that = (FileCacheKey) o;

      if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null) {
         return false;
      }
      if (indexName != null ? !indexName.equals(that.indexName) : that.indexName != null) {
         return false;
      }

      if (isLockKey != that.isLockKey) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      if (hashCode == 0) {
         hashCode = super.hashCode();
         hashCode = 31 * hashCode + (isLockKey ? 1 : 0);
      }
      return hashCode;
   }

   @Override
   public String toString() {
      return "CacheKey{" + "fileName='" + getFileName() + '\'' + "indexName='" + getIndexName() + '\'' + "isLockKey='"
               + isLockKey + '\'' + '}';
   }
}
