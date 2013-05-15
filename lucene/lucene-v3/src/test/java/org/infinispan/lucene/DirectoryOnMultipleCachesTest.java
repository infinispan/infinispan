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

import static org.infinispan.lucene.CacheTestSupport.assertTextIsFoundInIds;
import static org.infinispan.lucene.CacheTestSupport.writeTextToIndex;
import static org.infinispan.lucene.CacheTestSupport.optimizeIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies the Index can be spread across three different caches;
 * this is useful so that each cache can be configured independently
 * to better match the intended usage (like avoiding a CacheStore for volatile locking data).
 * 
 * @author Sanne Grinovero
 */
@SuppressWarnings("unchecked")
@Test(groups = "functional", testName = "lucene.DirectoryOnMultipleCachesTest")
public class DirectoryOnMultipleCachesTest {

   //timeout for test verifyIntendedLockCachesUsage()
   private static final long SLEEP = 60; //60 msecs
   private static final int MAX_ITERATIONS = 1000; //max timeout: SLEEP * MAX_ITERATIONS msecs (+- 60 seconds)

   private CacheContainer cacheManager;
   private Cache metadataCache;
   private Cache chunkCache;
   private Cache lockCache;

   @BeforeClass
   public void createBeforeClass() {
      cacheManager = CacheTestSupport.createLocalCacheManager();
      metadataCache = cacheManager.getCache("metadata");
      chunkCache = cacheManager.getCache("chunks");
      lockCache = cacheManager.getCache("locks");
   }
   
   @Test
   public void testRunningOnMultipleCaches() throws IOException {
      assert metadataCache != chunkCache;
      assert chunkCache != lockCache;
      assert lockCache != metadataCache;
      Directory dir = DirectoryBuilder.newDirectoryInstance(metadataCache, chunkCache, lockCache, "testingIndex").chunkSize(100).create();
      writeTextToIndex(dir, 0, "hello world");
      assertTextIsFoundInIds(dir, "hello", 0);
      writeTextToIndex(dir, 1, "hello solar system");
      assertTextIsFoundInIds(dir, "hello", 0, 1);
      assertTextIsFoundInIds(dir, "system", 1);
      optimizeIndex(dir);
      assertTextIsFoundInIds(dir, "hello", 0, 1);
      dir.close();
   }
   
   @Test(dependsOnMethods="testRunningOnMultipleCaches")
   public void verifyIntendedChunkCachesUsage() {
      int chunks = 0;
      for (Object key : chunkCache.keySet()) {
         chunks++;
         AssertJUnit.assertEquals(ChunkCacheKey.class, key.getClass());
         Object value = chunkCache.get(key);
         AssertJUnit.assertEquals(byte[].class, value.getClass());
      }
      assert chunks != 0;
   }
   
   @Test(dependsOnMethods="testRunningOnMultipleCaches")
   public void verifyIntendedLockCachesUsage() {
      final List<Object> keysThatShouldBeRemoved = new ArrayList<Object>();
      //all locks should be cleared now, so if any value is left it should be equal to one.
      for (Object key : lockCache.keySet()) {
         AssertJUnit.assertEquals(FileReadLockKey.class, key.getClass());
         int value = (Integer) lockCache.get(key);
         if (value == 0) {
            //zero means that key is removed. However the remove operation is done asynchronously so it can take some
            // time to be really removed from the lockCache.
            keysThatShouldBeRemoved.add(key);
            continue;
         }
         AssertJUnit.assertEquals(1, value);
      }
      for (int i = 0; i < MAX_ITERATIONS && !keysThatShouldBeRemoved.isEmpty(); ++i) {
         try {
            Thread.sleep(SLEEP);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
         }
         for (Iterator<Object> iterator = keysThatShouldBeRemoved.iterator(); iterator.hasNext(); ) {
            if (!lockCache.containsKey(iterator.next())) {
               //this key has been removed as expected
               iterator.remove();
            }
         }
      }
      AssertJUnit.assertTrue("The following keys " + keysThatShouldBeRemoved + " are supposed to be removed from lockCache",
                             keysThatShouldBeRemoved.isEmpty());
   }
   
   @Test(dependsOnMethods="testRunningOnMultipleCaches")
   public void verifyIntendedMetadataCachesUsage() {
      int metadata = 0;
      int filelists = 0;
      for (Object key : metadataCache.keySet()) {
         Object value = metadataCache.get(key);
         if (key.getClass().equals(org.infinispan.lucene.FileListCacheKey.class)) {
            filelists++;
            AssertJUnit.assertEquals(ConcurrentHashSet.class, value.getClass());
         }
         else if (key.getClass().equals(FileCacheKey.class)) {
            metadata++;
            AssertJUnit.assertEquals(FileMetadata.class, value.getClass());
         }
         else {
            AssertJUnit.fail("unexpected type of key in metadata cache: " + key.getClass());
         }
      }
      AssertJUnit.assertEquals(1, filelists);
      assert metadata != 0;
   }
   
   @AfterClass
   public void afterClass() {
      TestingUtil.killCacheManagers(cacheManager);
   }

}
