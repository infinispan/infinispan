package org.infinispan.lucene.cacheloader;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.cacheloader.configuration.LuceneLoaderConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verify we can write to a FSDirectory, and when using it via the {@link org.infinispan.lucene.cacheloader.LuceneCacheLoader}
 * we can find the same contents as by searching it directly.
 * This implicitly verifies configuration settings passed it, as it won't work if the CacheLoader
 * is unable to find the specific index path.
 *
 * @author Sanne Grinovero
 * @since 5.2
 */
@Test(groups = "functional", testName = "lucene.cachestore.IndexCacheLoaderTest")
public class IndexCacheLoaderTest {

   private static final int SCALE = 600;
   protected final String parentDir = TestingUtil.tmpDirectory(this.getClass());
   protected File rootDir = null;

   @BeforeMethod
   public void setUp() {
      rootDir = TestHelper.createRootDir(parentDir, getIndexPathName());
   }

   /**
    * @return a unique name for this test, where we store indexes
    */
   protected String getIndexPathName() {
      return this.getClass().getSimpleName();
   }

   @AfterMethod
   public void tearDown() {
      if(rootDir != null) {
         TestingUtil.recursiveFileRemove(rootDir);
      }
   }

   @Test
   public void testRescalingMath() {
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", 0, 1), 0);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", 1, 1), 1);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", 2, 1), 2);
      int MB = 1024*1024;
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", 0, MB), 0);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", 1, MB), 1);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", 2, MB), 1);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", MB, MB), 1);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", MB+1, MB), 2);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", MB+MB, MB), 2);
      Assert.assertEquals(DirectoryLoaderAdaptor.figureChunksNumber("", MB+MB+1, MB), 3);
   }

   @Test
   public void testReadExistingIndex() throws IOException {
      TestHelper.createIndex(rootDir, "index-A", 10 * SCALE, true);
      TestHelper.createIndex(rootDir, "index-B", 20 * SCALE, false);

      TestHelper.verifyIndex(rootDir, "index-A", 10 * SCALE, true);
      verifyDirectory(rootDir, "index-A", 10 * SCALE, true);

      TestHelper.verifyIndex(rootDir, "index-B", 20 * SCALE, false);
      verifyDirectory(rootDir, "index-B", 20 * SCALE, false);
   }

   private void verifyDirectory(final File rootDir, final String indexName, final int termsAdded, final boolean inverted) {
      final EmbeddedCacheManager cacheManager = initializeInfinispan(rootDir);
      TestingUtil.withCacheManager(new CacheManagerCallable(cacheManager) {
         @Override
         public void call() throws IOException {
            Cache<Object, Object> cache = cacheManager.getCache();
            Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, indexName).create();

            TestHelper.verifyOnDirectory(directory, termsAdded, inverted);
         }
      });
   }

   protected EmbeddedCacheManager initializeInfinispan(File rootDir) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .persistence()
            .addStore(LuceneLoaderConfigurationBuilder.class)
               .autoChunkSize(1024)
               .preload(true)
               .location(rootDir.getAbsolutePath());
      return TestCacheManagerFactory.createCacheManager(builder);
   }
}
