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

import java.util.Set;

import org.infinispan.Cache;
import org.testng.AssertJUnit;

/**
 * DirectoryIntegrityCheck contains helpers to assert assumptions we make on the structure of an
 * index as stored in an Infinispan cache.
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
@SuppressWarnings("unchecked")
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
      verifyDirectoryStructure(cache, indexName, false);
   }

   public static void verifyDirectoryStructure(Cache cache, String indexName, boolean wasAStressTest) {
      Set<String> fileList = (Set<String>) cache.get(new FileListCacheKey(indexName));
      AssertJUnit.assertNotNull(fileList);
      int fileListCacheKeyInstances = 0;
      for (Object key : cache.keySet()) {
         if (key instanceof ChunkCacheKey) {
            ChunkCacheKey existingChunkKey = (ChunkCacheKey) key;
            String filename = existingChunkKey.getFileName();
            AssertJUnit.assertEquals(existingChunkKey.getIndexName(), indexName);
            // the chunk must either match an entry in fileList or have a pending readLock:
//            if (fileList.contains(filename) == false) {
//               verifyReadlockExists(cache, indexName, filename);
//            }
            Object value = cache.get(existingChunkKey);
            AssertJUnit.assertNotNull(value);
            AssertJUnit.assertTrue(value instanceof byte[]);
            byte[] buffer = (byte[]) cache.get(existingChunkKey);
            AssertJUnit.assertTrue(buffer.length != 0);
         } else if (key instanceof FileCacheKey) {
            FileCacheKey fileCacheKey = (FileCacheKey) key;
            AssertJUnit.assertEquals(fileCacheKey.getIndexName(), indexName);
            String filename = fileCacheKey.getFileName();
//            if (fileList.contains(filename) == false) {
//               // if the file is not registered, assert that a readlock prevented it from being
//               // deleted:
//               verifyReadlockExists(cache, indexName, filename);
//            }
            Object value = cache.get(fileCacheKey);
            AssertJUnit.assertNotNull(value);
            AssertJUnit.assertTrue(value instanceof FileMetadata);
            FileMetadata metadata = (FileMetadata) value;
            long totalFileSize = metadata.getSize();
            long actualFileSize = deepCountFileSize(fileCacheKey, cache);
            AssertJUnit.assertEquals(actualFileSize, totalFileSize);
            AssertJUnit.assertTrue(fileCacheKey + " should not have existed", fileList.contains(fileCacheKey.getFileName()));
         } else if (key instanceof FileListCacheKey) {
            fileListCacheKeyInstances++;
            AssertJUnit.assertEquals(1, fileListCacheKeyInstances);
         } else if (key instanceof FileReadLockKey) {
            /*//FIXME testcase to be fixed after ISPN-616 
            FileReadLockKey readLockKey = (FileReadLockKey) key;
            Assert.assertEquals(readLockKey.getIndexName(), indexName);
            Object value = cache.get(readLockKey);
            // we verify that a ReadLock exists only for existing files
            Assert.assertTrue(cache.get(new FileCacheKey(indexName, readLockKey.getFileName())) != null, key + " left over from deleted "
                     + readLockKey.getFileName());
            Assert.assertTrue(cache.get(new ChunkCacheKey(indexName, readLockKey.getFileName(), 0)) != null);
            Assert.assertTrue(fileList.contains(readLockKey.getFileName()), "readlock still exists but the file was deleted: "
                     + readLockKey);
            Assert.assertTrue(value == null || value.equals(1));
            */
         } else {
            AssertJUnit.fail("an unexpected key was found in the cache having key type " + key.getClass() + " toString:" + key);
         }
      }
   }

   private static void verifyReadlockExists(Cache cache, String indexName, String filename) {
      FileReadLockKey readLockKey = new FileReadLockKey(indexName, filename);
      Object readLockValue = cache.get(readLockKey);
      AssertJUnit.assertNotNull(readLockValue);
      AssertJUnit.assertTrue(readLockValue instanceof Integer);
      int v = ((Integer) readLockValue).intValue();
      AssertJUnit.assertTrue("readlock exists for unregistered file of unexpected value: " + v + " for file: " + filename, v > 1);
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
      FileMetadata metadata = (FileMetadata) cache.get(fileCacheKey);
      int bufferSize = metadata.getBufferSize();
      for (int i = 0;; i++) {
         ChunkCacheKey chunkKey = new ChunkCacheKey(indexName, fileName, i, bufferSize);
         byte[] buffer = (byte[]) cache.get(chunkKey);
         if (buffer == null) {
            AssertJUnit.assertFalse(cache.containsKey(chunkKey));
            return accumulator;
         } else {
            assert buffer.length > 0; //check we don't store useless data
            accumulator += buffer.length;
         }
      }
   }
   
   public static void assertFileNotExists(Cache cache, String indexName, String fileName, long maxWaitForCondition) throws InterruptedException {
      Set<String> fileList = (Set<String>) cache.get(new FileListCacheKey(indexName));
      AssertJUnit.assertNotNull(fileList);
      AssertJUnit.assertFalse(fileList.contains(fileName)); //check is in sync: no waiting allowed in this case
      boolean allok = false;
      while (maxWaitForCondition >= 0 && !allok) {
         Thread.sleep(10);
         maxWaitForCondition -= 10;
         allok=true;
         FileMetadata metadata = (FileMetadata) cache.get(new FileCacheKey(indexName, fileName));
         if (metadata!=null) allok=false;
         for (int i = 0; i < 100; i++) {
            //bufferSize set to 0 as metadata might not be available, and it's not part of equals/hashcode anyway.
            ChunkCacheKey key = new ChunkCacheKey(indexName, fileName, i, 0);
            if (cache.get(key)!=null) allok=false;
         }
      }
      AssertJUnit.assertTrue(allok);
   }

   /**
    * Verified the file exists and has a specified value for readLock;
    * Consider that null should be interpreted as value 1;
    */
   public static void assertFileExistsHavingRLCount(Cache cache, String fileName, String indexName, int expectedReadcount, int chunkSize, boolean expectRegisteredInFat) {
      Set<String> fileList = (Set<String>) cache.get(new FileListCacheKey(indexName));
      AssertJUnit.assertNotNull(fileList);
      AssertJUnit.assertTrue(fileList.contains(fileName) == expectRegisteredInFat);
      FileMetadata metadata = (FileMetadata) cache.get(new FileCacheKey(indexName, fileName));
      AssertJUnit.assertNotNull(metadata);
      long totalFileSize = metadata.getSize();
      int chunkNumbers = (int)(totalFileSize / chunkSize);
      for (int i = 0; i < chunkNumbers; i++) {
         AssertJUnit.assertNotNull(cache.get(new ChunkCacheKey(indexName, fileName, i, metadata.getBufferSize())));
      }
      FileReadLockKey readLockKey = new FileReadLockKey(indexName,fileName);
      Object value = cache.get(readLockKey);
      if (expectedReadcount <= 1) {
         AssertJUnit.assertTrue("readlock value is " + value, value == null || Integer.valueOf(1).equals(value));
      }
      else {
         AssertJUnit.assertNotNull(value);
         AssertJUnit.assertTrue(value instanceof Integer);
         int v = (Integer)value;
         AssertJUnit.assertEquals(v, expectedReadcount);
      }
   }

}
