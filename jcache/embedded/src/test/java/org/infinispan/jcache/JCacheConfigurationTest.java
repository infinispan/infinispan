package org.infinispan.jcache;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.net.URI;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertTrue;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "jcache.JCacheConfigurationTest")
public class JCacheConfigurationTest {

   public void testNamedCacheConfiguration() {
      withCacheManager(new CacheManagerCallable(
            TestCacheManagerFactory.createCacheManager(false)) {
         @Override
         public void call() {
            cm.defineConfiguration("oneCache", new ConfigurationBuilder().build());
            JCacheManager jCacheManager = new JCacheManager(URI.create("oneCacheManager"), cm, null);
            assertTrue(null != jCacheManager.getCache("oneCache"));
         }
      });
   }

}
