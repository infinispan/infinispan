/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
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

package org.infinispan.lucene.cacheloader;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.lucene.ChunkCacheKey;
import org.infinispan.lucene.FileCacheKey;
import org.infinispan.lucene.FileListCacheKey;
import org.infinispan.lucene.FileReadLockKey;
import org.infinispan.lucene.cachestore.LuceneCacheLoader;
import org.infinispan.lucene.cachestore.LuceneCacheLoaderConfig;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2013 Red Hat Inc.
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucene.cacheloader.CacheLoaderAPITest")

public class CacheLoaderAPITest extends SingleCacheManagerTest {

   private static final String rootDirectoryName = "CacheLoaderAPITest.indexesRootDirTmp";
   private static final String indexName = "index-A";
   private static final int elementCount = 10;
   protected final String parentDir = ".";
   private File rootDir;

   public CacheLoaderAPITest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      rootDir = new File(new File(parentDir), rootDirectoryName);
      File subDir = new File(rootDir, indexName);
      boolean directoriesCreated = subDir.mkdirs();
      assert directoriesCreated : "couldn't create directory for test";
      //We need at least one Directory to exist on filesystem to trigger the problem
      FSDirectory luceneDirectory = FSDirectory.open(subDir);
      luceneDirectory.close();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .loaders()
            .addLoader()
               .cacheLoader( new LuceneCacheLoader() )
                  .addProperty(LuceneCacheLoaderConfig.LOCATION_OPTION, rootDir.getAbsolutePath())
                  .addProperty(LuceneCacheLoaderConfig.AUTO_CHUNK_SIZE_OPTION, "110");
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void filteredKeyLoadTest() throws CacheLoaderException {
      CacheLoaderManager cacheLoaderManager = TestingUtil.extractComponent(cache, CacheLoaderManager.class);
      CacheLoader loader = cacheLoaderManager.getCacheLoader();
      Assert.assertNotNull(loader);
      Assert.assertTrue(loader instanceof LuceneCacheLoader);
      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) loader;
      cacheLoader.loadAllKeys(null);
   }

   @Test(dataProvider = "passEntriesCount")
   public void testLoadEntries(int entriesNum) throws Exception {
      TestHelper.createIndex(rootDir, indexName, elementCount, true);
      String[] fileNamesFromIndexDir = TestHelper.getFileNamesFromDir(rootDir, indexName);

      LuceneCacheLoader cacheLoader = (LuceneCacheLoader) TestingUtil.extractComponent(cacheManager.getCache(),
                                                                                       CacheLoaderManager.class).getCacheLoader();

      Set<InternalCacheEntry> loadedEntrySet = null;
      if(entriesNum > 0) {
         loadedEntrySet = cacheLoader.load(entriesNum);
      } else {
         loadedEntrySet = cacheLoader.loadAll();
      }

      if (entriesNum < elementCount && entriesNum > 0) {
         AssertJUnit.assertEquals(entriesNum, loadedEntrySet.size());
      } else {
         for(String fileName : fileNamesFromIndexDir) {
            FileCacheKey key = new FileCacheKey(indexName, fileName);
            assert cacheLoader.load(key) != null;

            boolean found = false;
            for(InternalCacheEntry entry : loadedEntrySet) {
               FileCacheKey keyFromLoad = null;

               if(entry.getKey() instanceof FileCacheKey) {
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
   }

   @DataProvider(name = "passEntriesCount")
   public Object[][] provideEntriesCount() {
      return new Object[][]{
            {new Integer(elementCount + 5)},
            {new Integer(elementCount - 5)},
            {new Integer(0)}
      };
   }

   @Override
   protected void teardown() {
      super.teardown();
      File rootDir = new File(new File(parentDir), rootDirectoryName);
      TestingUtil.recursiveFileRemove(rootDir);
   }

}
