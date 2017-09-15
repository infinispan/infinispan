package org.infinispan.lucene.readlocks;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies a DistributedSegmentReadLocker can be built only on certain types of caches,
 * for example it shouldn't be allowed to use eviction: see ISPN-680
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "lucene.readlocks.ConfigurationCheckTest")
public class ConfigurationCheckTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configurationBuilder = CacheTestSupport.createLocalCacheConfiguration();
      configurationBuilder
         .memory().size(10L);
      return TestCacheManagerFactory.createCacheManager(configurationBuilder);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testEvictionIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, c, c, "lucene.readlocks.ConfigurationCheckTest", -1);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testLocksCacheNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker(null, c, c, "lucene.readlocks.ConfigurationCheckTest", -1);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testChunkCacheNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, null, c, "lucene.readlocks.ConfigurationCheckTest", -1);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testMetaDataNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, c, null, "lucene.readlocks.ConfigurationCheckTest", -1);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIndexNameNullIsNotAllowed() {
      Cache<?, ?> c = cacheManager.getCache();
      new DistributedSegmentReadLocker((Cache<Object, Integer>) c, c, c, null, -1);
   }

}
