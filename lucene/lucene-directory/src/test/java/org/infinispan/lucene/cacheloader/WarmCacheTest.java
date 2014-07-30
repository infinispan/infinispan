package org.infinispan.lucene.cacheloader;

import org.apache.lucene.store.Directory;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.lucene.cacheloader.configuration.LuceneLoaderConfigurationBuilder;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.lucene.testutils.LuceneUtils;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

import static org.infinispan.lucene.cacheloader.TestHelper.createIndex;
import static org.infinispan.test.TestingUtil.findInterceptor;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 *
 * Verify that preloading a read only lucene index in a cache and query it
 * afterwards will not cause cache loader hits
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "lucene.cacheloader.WarmCacheTest")
public class WarmCacheTest extends MultipleCacheManagersTest {

   public static final int CLUSTER_SIZE = 3;
   public static final String INDEX_NAME = "INDEX";
   public static final int TERMS_NUMBER = 1000;
   private File indexDir;

   @Test
   public void shouldNotHitCacheLoaderWhenWarm() throws Throwable {
      Cache<Object, Object> cache = cacheManagers.get(0).getCache();
      Directory directory = DirectoryBuilder.newDirectoryInstance(cache, cache, cache, INDEX_NAME).create();

      assertTrue(LuceneUtils.readTerms("main", directory).contains("500"));
      assertEquals(TERMS_NUMBER + 1, LuceneUtils.numDocs(directory));
      LuceneUtils.collect(directory, TERMS_NUMBER);

      assertNoCacheLoaderInteractions();
   }

   private void assertNoCacheLoaderInteractions() {
      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         CacheLoaderInterceptor interceptor = findInterceptor(cacheManager.getCache(), CacheLoaderInterceptor.class);
         assertEquals(0, interceptor.getCacheLoaderLoads());
         assertEquals(0, interceptor.getCacheLoaderMisses());
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      indexDir = Files.createTempDirectory("test-").toFile();
      createIndex(indexDir, INDEX_NAME, TERMS_NUMBER, false);
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_ASYNC, false);
      c.clustering().hash().numOwners(1);
      c.jmxStatistics().enable();
      c.persistence()
            .addStore(LuceneLoaderConfigurationBuilder.class)
            .preload(true)
            .location(indexDir.getAbsolutePath());
      createCluster(c, CLUSTER_SIZE);
      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(indexDir);
   }

}
