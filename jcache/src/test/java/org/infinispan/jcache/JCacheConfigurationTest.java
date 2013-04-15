package org.infinispan.jcache;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests JCache and JCacheManager configuration.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "jcache.JCacheConfigurationTest")
public class JCacheConfigurationTest {

   public void testNamedCacheConfiguration() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createLocalCacheManager(false)) {
         @Override
         public void call() {
            cm.defineConfiguration("oneCache", new ConfigurationBuilder().build());
            JCacheManager jCacheManager = new JCacheManager("oneCacheManager", cm);
            assertTrue(null != jCacheManager.getCache("oneCache"));
         }
      });
   }

}
