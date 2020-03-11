package org.infinispan.lucene.cacheloader;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests covering LuceneCacheLoader methods.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "lucene.cacheloader.LuceneCacheLoaderTest")
public class LuceneCacheLoaderTest extends IndexCacheLoaderTest {
   private static final Log log = LogFactory.getLog(LuceneCacheLoaderTest.class);

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
            @Override
            public void call() {
               Directory directory = null;
               try {
                  Cache cache = cacheManager.getCache(CACHE_NAME);
                  directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
               } finally {
                  if(directory != null) {
                     try {
                        directory.close();
                     } catch (IOException e) {
                        log.warnf(e, "Error closing directory %s", directory);
                     }
                  }
               }
            }
         });
      } finally {
         if(file != null) Util.recursiveFileRemove(file);
      }
   }

   public void testLuceneCacheLoaderWithNonReadableDir() throws IOException {
      boolean isReadOff = rootDir.setReadable(false);
      if(isReadOff) {
         final EmbeddedCacheManager cacheManager = initializeInfinispan(rootDir);
         TestingUtil.withCacheManager(new CacheManagerCallable(cacheManager) {
            @Override
            public void call() {
               try {
                  Cache cache = cacheManager.getCache(CACHE_NAME);
                  DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();
               } catch(Exception ex) {
                  assert ex instanceof CacheException;
               } finally {
                  rootDir.setReadable(true);
               }
            }
         });
      } else {
         log.info("Skipping test because it is not possible to make the directory non-readable, i.e. because the tests are run with the root user.");
      }
   }

   public void testContainsKeyWithNoExistentRootDir() {
      final File rootDir = new File(new File(parentDir).getAbsoluteFile(), getIndexPathName() + "___");
      final EmbeddedCacheManager cacheManager = initializeInfinispan(rootDir);
      try {
         TestingUtil.withCacheManager(new CacheManagerCallable(cacheManager) {
            @Override
            public void call() throws Exception {
               Cache cache = cacheManager.getCache(CACHE_NAME);
               Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();

               TestHelper.createIndex(rootDir, indexName, elementCount, true);
               TestHelper.verifyOnDirectory(directory, elementCount, true);

               String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

               LuceneCacheLoader cacheLoader = TestingUtil.getFirstLoader(cacheManager.getCache(CACHE_NAME));
               for(String fileName : fileNamesFromIndexDir) {
                  FileCacheKey key = new FileCacheKey(indexName, fileName, -1);
                  assert cacheLoader.contains(key);

                  //Testing non-existent keys with non-acceptable type
                  assert !cacheLoader.contains(fileName);
               }
            }
         });
      } finally {
         Util.recursiveFileRemove(rootDir);
      }
   }
}
