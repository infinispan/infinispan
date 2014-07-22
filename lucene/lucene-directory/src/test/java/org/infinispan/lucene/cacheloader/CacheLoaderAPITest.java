package org.infinispan.lucene.cacheloader;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.lucene.cacheloader.configuration.LuceneLoaderConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileReadLockKey;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2013 Red Hat Inc.
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucene.cachestore.CacheLoaderAPITest")
public class CacheLoaderAPITest extends SingleCacheManagerTest {

   private static final String rootDirectoryName = "CacheLoaderAPITest.indexesRootDirTmp";
   private static final String indexName = "index-A";
   private static final int elementCount = 10;
   protected final String parentDir = TestingUtil.tmpDirectory(this.getClass());
   private File rootDir;

   public CacheLoaderAPITest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      rootDir = new File(new File(parentDir).getAbsoluteFile(), rootDirectoryName);
      boolean rootDirCreated = rootDir.mkdirs();

      assert rootDirCreated : "couldn't created root directory!";

      File subDir = new File(rootDir, indexName);
      boolean directoriesCreated = subDir.mkdir();
      assert directoriesCreated : "couldn't create directory for test";

      //We need at least one Directory to exist on filesystem to trigger the problem
      FSDirectory luceneDirectory = FSDirectory.open(subDir);
      luceneDirectory.close();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.persistence()
            .addStore(LuceneLoaderConfigurationBuilder.class)
               .preload(true)
               .autoChunkSize(110)
               .location(rootDir.getAbsolutePath());

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testFilteredKeyLoad() {
      CacheLoader loader = TestingUtil.getFirstLoader(cache);
      AssertJUnit.assertNotNull(loader);
      AssertJUnit.assertTrue(loader instanceof LuceneCacheLoader);
      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) loader;
      PersistenceUtil.count(cacheLoader, null);
   }

   public void testLoadAllKeysWithExclusion() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);

      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());

      Set keyList = PersistenceUtil.toKeySet(cacheLoader, null);
      int initialCount = keyList.size();

      HashSet exclusionSet = new HashSet();
      for(String fileName : fileNamesFromIndexDir) {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         AssertJUnit.assertNotNull(cacheLoader.load(key));

         exclusionSet.add(key);
      }

      keyList = PersistenceUtil.toKeySet(cacheLoader, new CollectionKeyFilter(exclusionSet));

      AssertJUnit.assertEquals((initialCount - fileNamesFromIndexDir.length), keyList.size());

      Iterator it = keyList.iterator();
      if(it.hasNext()) {
         assert !(it.next() instanceof FileCacheKey);
      }
   }

   public void testContainsKeyWithNoExistentRootDir() throws IOException {
      Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();

      TestHelper.createIndex(rootDir, indexName, elementCount, true);
      TestHelper.verifyOnDirectory(directory, elementCount, true);

      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());
      for(String fileName : fileNamesFromIndexDir) {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         assert cacheLoader.contains(key);

         //Testing non-existent keys with non-acceptable type
         assert !cacheLoader.contains(fileName);
      }

   }

   public void testContainsKeyCacheKeyTypes() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());

      assert cacheLoader.contains(new FileListCacheKey(indexName));

      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);
      for(String fileName : fileNamesFromIndexDir) {
         assert !cacheLoader.contains(new FileReadLockKey(indexName, fileName)) : "Failed for " + fileName;
         assert cacheLoader.contains(new ChunkCacheKey(indexName, fileName, 0, 1024)) : "Failed for " + fileName;
      }

      assert !cacheLoader.contains(new ChunkCacheKey(indexName, "testFile.txt", 0, 1024));
   }

   public void testLoadKey() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);

      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());
      for(String fileName : fileNamesFromIndexDir) {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         AssertJUnit.assertNotNull(cacheLoader.load(key));

         //Testing non-existent keys with non-acceptable type
         AssertJUnit.assertNull(cacheLoader.load(fileName));
      }
   }

   @Test(expectedExceptions = PersistenceException.class)
   public void testLoadKeyWithNonExistentFile() throws Exception {
      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());
      FileCacheKey key = new FileCacheKey(indexName, "testKey");
      AssertJUnit.assertNull(cacheLoader.load(key));
   }

   public void testLoadKeyWithInnerNonReadableDir() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);
      File innerDir = new File(rootDir.getAbsoluteFile(), "index-B");
      try {
         boolean created = innerDir.mkdir();
         assert created;

         boolean isReadoff = innerDir.setReadable(false);
         boolean isWriteoff = innerDir.setWritable(false);

         if (isReadoff && isWriteoff) {
            LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());
            cacheLoader.load(5);
         } else {
            log.info("Skipping test because it is not possible to make the directory non-readable, i.e. because the tests are run with the root user.");
         }
      } catch(Exception ex) {
         assert ex instanceof PersistenceException;
      } finally {
         innerDir.setReadable(true);
         innerDir.setWritable(true);
      }
   }

   public void testLoadEntries() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);
      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());

      Set<InternalCacheEntry> loadedEntrySet =
            PersistenceUtil.toEntrySet(cacheLoader, null, cache.getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class));

      for (String fileName : fileNamesFromIndexDir) {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         AssertJUnit.assertNotNull(cacheLoader.load(key));

         boolean found = false;
         for (InternalCacheEntry entry : loadedEntrySet) {
            FileCacheKey keyFromLoad = null;

            if (entry.getKey() instanceof FileCacheKey) {
               keyFromLoad = (FileCacheKey) entry.getKey();

               if (keyFromLoad != null && keyFromLoad.equals(key)) {
                  found = true;
                  break;
               }
            }
         }

         assert found : "No corresponding entry found for " + key;
      }
   }

   public void testLoadAllKeys() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);
      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());

      Set keyList = PersistenceUtil.toKeySet(cacheLoader, null);
      for(String fileName : fileNamesFromIndexDir) {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         AssertJUnit.assertNotNull(cacheLoader.load(key));

         boolean found = false;
         for(Object keyFromList : keyList) {
            if(keyFromList instanceof FileCacheKey && keyFromList.equals(key)) {
               found = true;
               break;
            }
         }

         assert found : "No corresponding key was found for " + key;
      }
   }

   public void testLoadAllKeysWithExclusionOfRootKey() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());
      Set keySet = PersistenceUtil.toKeySet(cacheLoader, null);
      int initialCount = keySet.size();

      HashSet exclusionSet = new HashSet();
      exclusionSet.add(new FileListCacheKey(indexName));

      keySet = PersistenceUtil.toKeySet(cacheLoader, new CollectionKeyFilter(exclusionSet));
      String[] fileNamesArr = TestHelper.getFileNamesFromDir(rootDir, indexName);
      AssertJUnit.assertEquals((initialCount - 1), keySet.size());

      Iterator it = keySet.iterator();
      while (it.hasNext()) {
         assert !(it.next() instanceof FileListCacheKey);
      }
   }

   public void testLoadAllKeysWithChunkExclusion() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);
      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());

      HashSet exclusionSet = new HashSet();
      String[] fileNames = TestHelper.getFileNamesFromDir(rootDir, indexName);
      for(String fileName : fileNames) {
         exclusionSet.add(new ChunkCacheKey(indexName, fileName, 0, 110));
      }

      Set keyList = PersistenceUtil.toKeySet(cacheLoader, null);
      checkIfExists(keyList, exclusionSet, true, false);

      keyList = PersistenceUtil.toKeySet(cacheLoader, new CollectionKeyFilter(exclusionSet));
      checkIfExists(keyList, exclusionSet, false, true);
   }

   @Test
   public void testLoadAllKeysWithNullExclusion() throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);

      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.getFirstLoader(cacheManager.getCache());

      Set keyList = PersistenceUtil.toKeySet(cacheLoader, null);

      for(String fileName : fileNamesFromIndexDir) {
         FileCacheKey key = new FileCacheKey(indexName, fileName);
         AssertJUnit.assertNotNull(cacheLoader.load(key));

         boolean found = false;
         for(Object keyFromList : keyList) {
            if(keyFromList instanceof FileCacheKey && keyFromList.equals(key)) {
               found = true;
               break;
            }
         }

         assert found : "No corresponding key was found for " + key;
      }
   }

   @Override
   protected void teardown() {
      TestingUtil.recursiveFileRemove(rootDir);
      super.teardown();
   }

   private void checkIfExists(Set result, Set exclusionSet, boolean shouldExist, boolean allShouldBeChecked) {
      boolean keyExists = false;
      for(Object obj : exclusionSet) {
         ChunkCacheKey key = (ChunkCacheKey) obj;

         boolean exists = false;
         for(Object expectedChunk : result) {
            if(obj.equals(expectedChunk)) {
               exists = true;
               break;
            }
         }

         keyExists = exists;

         if(!allShouldBeChecked && exists) {
            break;
         }
      }

      AssertJUnit.assertEquals(shouldExist, keyExists);
   }

}
