/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.lucene.cachestore;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.cachestore.LuceneCacheLoader;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

/**
 * Tests covering LuceneCacheLoader methods.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.cacheloader.LuceneCacheLoaderTest")
public class LuceneCacheLoaderTest extends IndexCacheLoaderTest {

   private String indexName = "index-A";
   private int elementCount = 10;

   @Test(expectedExceptions = CacheException.class)
   public void testLuceneCacheLoaderWithWrongDir() throws IOException {
      File file = new File("test.txt");
      boolean created = file.createNewFile();
      file.deleteOnExit();

      assert created;

      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = initializeInfinispan(file);
         Cache cache = cacheManager.getCache();
         Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
      } finally {
         if(cacheManager != null) {
            TestingUtil.killCacheManagers(cacheManager);
         }
      }
   }

   @Test(expectedExceptions = CacheException.class)
   public void testLuceneCacheLoaderWithNonReadableDir() throws IOException {
      rootDir.setReadable(false);

      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = initializeInfinispan(rootDir);
         Cache cache = cacheManager.getCache();
         DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
      } finally {
         if(cacheManager != null) {
            TestingUtil.killCacheManagers(cacheManager);
         }
         rootDir.setReadable(true);
      }
   }

   public void testContainsKeyWithNoExistentRootDir() throws IOException, CacheLoaderException {
      File rootDir = new File(new File(parentDir), rootDirectoryName + "___");
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = initializeInfinispan(rootDir);
         Cache cache = cacheManager.getCache();
         Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();

         TestHelper.createIndex(rootDir, indexName, elementCount, true);
         TestHelper.verifyOnDirectory(directory, elementCount, true);

         String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

         LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.extractComponent(cacheManager.getCache(),
                                                                                          CacheLoaderManager.class).getCacheLoader();
         for(String fileName : fileNamesFromIndexDir) {
            FileCacheKey key = new FileCacheKey(indexName, fileName);
            assert cacheLoader.containsKey(key);

            //Testing non-existent keys with non-acceptable type
            assert !cacheLoader.containsKey(fileName);
         }

      } finally {
         if(cacheManager != null) {
            TestingUtil.killCacheManagers(cacheManager);
         }
         TestingUtil.recursiveFileRemove(rootDir);
      }
   }
}
