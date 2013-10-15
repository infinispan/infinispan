package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.support.DelegatingCacheLoader;
import org.infinispan.persistence.support.DelegatingCacheWriter;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test(groups = "functional", testName = "persistence.InitCountTest")
public class InitCountTest extends AbstractCacheTest {

   public void testInitInvocationCountNoDelegate() {
      test(false);
   }

   public void testInitInvocationCountWithDelegate() {
      test(true);
   }

   /**
    * @param async set to true means the cache store instance is placed under an DelegatingCacheWriter
    */
   private void test(boolean async) {
      ConfigurationBuilder dcc = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      DummyInMemoryStoreConfigurationBuilder builder = dcc.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      if (async) {
         builder.async().enable();
      }
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(dcc);
      Cache<Object,Object> cache = cacheManager.getCache();

      try {
         CacheLoader firstLoader = TestingUtil.getFirstLoader(cache);
         CacheLoader undelegatedLoader = firstLoader instanceof DelegatingCacheLoader ? ((DelegatingCacheLoader) firstLoader).undelegate() : firstLoader;
         CacheWriter firstWriter = TestingUtil.getFirstWriter(cache);
         CacheWriter undelegatedWriter = firstWriter instanceof DelegatingCacheWriter ? ((DelegatingCacheWriter) firstWriter).undelegate() : firstWriter;
         assertEquals(1, ((DummyInMemoryStore)undelegatedLoader).initCount.get());
         assertEquals(1, ((DummyInMemoryStore)undelegatedWriter).initCount.get());
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

}
