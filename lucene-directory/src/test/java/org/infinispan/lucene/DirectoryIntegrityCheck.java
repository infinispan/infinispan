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

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileMetadata;
import org.testng.Assert;

/**
 * DirectoryIntegrityCheck contains helpers to assert assumptions we make on the structure of an
 * index as stored in an Infinispan cache.
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
public class DirectoryIntegrityCheck {

   /**
    * Verifies that no garbage elements are left over in the cache and that for each type of object
    * the expected value is stored. Also asserts for proper size metadata comparing to actual bytes
    * used in chunks. It's assumed that only one index is stored in the inspected cache, and that
    * the index is not being used by IndexReaders or IndexWriters.
    * 
    * @param cache
    *           The cache to inspect
    * @param indexName
    *           The name of the unique index stored in the cache
    */
   public static void verifyDirectoryStructure(Cache cache, String indexName) {
      Set<String> fileList = (Set<String>) cache.get(new FileListCacheKey(indexName));
      Assert.assertNotNull(fileList);
      int fileListCacheKeyInstances = 0;
      for (Object key : cache.keySet()) {
         if (key instanceof ChunkCacheKey) {
            ChunkCacheKey existingChunkKey = (ChunkCacheKey) key;
            String filename = existingChunkKey.getFileName();
            Assert.assertEquals(existingChunkKey.getIndexName(), indexName);
            Assert.assertTrue(fileList.contains(filename));
            Object value = cache.get(existingChunkKey);
            Assert.assertNotNull(value);
            Assert.assertTrue(value instanceof byte[]);
            byte[] buffer = (byte[]) cache.get(existingChunkKey);
            Assert.assertTrue(buffer.length != 0);
         } else if (key instanceof FileCacheKey) {
            FileCacheKey fileCacheKey = (FileCacheKey) key;
            Assert.assertEquals(fileCacheKey.getIndexName(), indexName);
            Assert.assertTrue(fileList.contains(fileCacheKey.getFileName()), fileCacheKey + " should not have existed");
            Object value = cache.get(fileCacheKey);
            Assert.assertNotNull(value);
            Assert.assertTrue(value instanceof FileMetadata);
            FileMetadata metadata = (FileMetadata) value;
            long totalFileSize = metadata.getSize();
            long actualFileSize = deepCountFileSize(fileCacheKey, cache);
            Assert.assertEquals(actualFileSize, totalFileSize);
         } else if (key instanceof FileListCacheKey) {
            fileListCacheKeyInstances++;
            Assert.assertEquals(1, fileListCacheKeyInstances);
         } else if (key instanceof FileReadLockKey) {
            FileReadLockKey readLockKey = (FileReadLockKey) key;
            Assert.assertEquals(readLockKey.getIndexName(), indexName);
            Assert.assertTrue(fileList.contains(readLockKey.getFileName()), readLockKey + " should not have existed");
            Object value = cache.get(readLockKey);
            Assert.assertNotNull(value);
            Assert.assertTrue(value instanceof Integer);
            int readLockCount = (Integer) value;
            Assert.assertEquals(readLockCount, 1, " for FileReadLockKey " + readLockKey);
         } else {
            Assert.fail("an unexpected key was found in the cache having key type " + key.getClass() + " toString:" + key);
         }
      }
   }

   /**
    * For a given FileCacheKey return the total size of all chunks related to the file.
    * 
    * @param fileCacheKey
    *           the key to the file to inspect
    * @param cache
    *           the cache storing the chunks
    * @return the total size adding all found chunks up
    */
   public static long deepCountFileSize(FileCacheKey fileCacheKey, Cache cache) {
      String indexName = fileCacheKey.getIndexName();
      String fileName = fileCacheKey.getFileName();
      long accumulator = 0;
      for (int i = 0;; i++) {
         ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, fileName, i);
         byte[] buffer = (byte[]) cache.get(chunkKey);
         if (buffer == null) {
            return accumulator;
         } else {
            accumulator += buffer.length;
         }
      }
   }

}
