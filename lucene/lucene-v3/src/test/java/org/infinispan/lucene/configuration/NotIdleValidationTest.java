package org.infinispan.lucene.configuration;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.lucene.CacheTestSupport;
import org.infinispan.lucene.directory.DirectoryBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifies that the Lucene Directory correctly refuses to use a
 * Cache for which a maximum idle time is activated.
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "lucene.configuration.NotIdleValidationTest")
public class NotIdleValidationTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = CacheTestSupport.createLocalCacheConfiguration();
      cfg.expiration().maxIdle(10l);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp =
         "ISPN(\\d)*: Lucene Directory for index 'testIndexBeta' can not use Cache '___defaultcache': expiration idle time enabled on the Cache configuration!")
   public void failOnExpiry() {
      DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "testIndexBeta").create();
   }

}
