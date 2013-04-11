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
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
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
      File file = null;

      try {
         file = new File(new File(parentDir).getAbsoluteFile(), "test.txt");
         boolean created = file.createNewFile();
         file.deleteOnExit();

         assert created;

         final EmbeddedCacheManager cacheManager = initializeInfinispan(file);
         TestingUtil.withCacheManager(new CacheManagerCallable(cacheManager) {
            public void call() {
               Directory directory = null;
               try {
                  Cache cache = cacheManager.getCache();
                  directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
               } finally {
                  if(directory != null) {
                     try {
                        directory.close();
                     } catch (IOException e) {
                        e.printStackTrace();
                     }
                  }
               }
            }
         });
      } finally {
         if(file != null) TestingUtil.recursiveFileRemove(file);
      }
   }

   public void testLuceneCacheLoaderWithNonReadableDir() throws IOException {
      boolean isReadOff = rootDir.setReadable(false);
      if(isReadOff) {
         final EmbeddedCacheManager cacheManager = initializeInfinispan(rootDir);
         TestingUtil.withCacheManager(new CacheManagerCallable(cacheManager) {
            public void call() {
               try {
                  Cache cache = cacheManager.getCache();
                  DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
               } catch(Exception ex) {
                  assert ex instanceof CacheException;
               } finally {
                  rootDir.setReadable(true);
               }
            }
         });
      } else {
         System.out.println("The test is executed only if it is possible to make the directory non-readable. I.e. the tests are run not under the root.");
      }
   }

   public void testContainsKeyWithNoExistentRootDir() {
      final File rootDir = new File(new File(parentDir).getAbsoluteFile(), getIndexPathName() + "___");
      final EmbeddedCacheManager cacheManager = initializeInfinispan(rootDir);
      try {
         TestingUtil.withCacheManager(new CacheManagerCallable(cacheManager) {
            public void call() {
               Cache cache = cacheManager.getCache();
               Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();

               try {
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
               } catch(Exception ex) {
                  throw new RuntimeException(ex);
               }
            }
         });
      } finally {
         TestingUtil.recursiveFileRemove(rootDir);
      }
   }
}
