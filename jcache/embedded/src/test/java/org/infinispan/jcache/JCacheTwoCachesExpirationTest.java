package org.infinispan.jcache;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import javax.cache.Cache;
import java.lang.reflect.Method;
import java.net.URI;

/**
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.JCacheTwoCachesExpirationTest", groups = "functional")
@CleanupAfterMethod
public class JCacheTwoCachesExpirationTest extends AbstractTwoCachesExpirationTest {

   @Override
   public Cache getCache1(Method m) {
      JCacheManager jCacheManager = new JCacheManager(URI.create(m.getName()), cacheManagers.get(0), null);
      return jCacheManager.getCache("expiry");
   }

   @Override
   public Cache getCache2(Method m) {
      JCacheManager jCacheManager = new JCacheManager(URI.create(m.getName()), cacheManagers.get(1), null);
      return jCacheManager.getCache("expiry");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultClusteredCacheConfig2 = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      defaultClusteredCacheConfig2.expiration().lifespan(2000);
      createClusteredCaches(2, "expiry", defaultClusteredCacheConfig2);
      waitForClusterToForm("expiry");
   }
}
