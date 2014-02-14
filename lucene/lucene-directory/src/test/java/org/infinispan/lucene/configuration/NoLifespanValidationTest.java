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
 * Cache for which expiry is activated.
 *
 * @author Sanne Grinovero
 */
@Test(groups = "functional", testName = "lucene.configuration.NoLifespanValidationTest")
public class NoLifespanValidationTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = CacheTestSupport.createLocalCacheConfiguration();
      cfg.expiration().lifespan(10l);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp =
         "ISPN(\\d)*: Lucene Directory for index 'testIndexAlpha' can not use Cache '___defaultcache': maximum lifespan enabled on the Cache configuration!")
   public void failOnExpiry() {
      DirectoryBuilder.newDirectoryInstance(cache, cache, cache, "testIndexAlpha").create();
   }

}
