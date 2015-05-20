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
@Test(testName = "org.infinispan.jcache.JCacheTwoCachesBasicOpsTest", groups = "functional")
@CleanupAfterMethod
public class JCacheTwoCachesBasicOpsTest extends AbstractTwoCachesBasicOpsTest {

   @Override
   public Cache getCache1(Method m) {
      JCacheManager jCacheManager = new JCacheManager(URI.create(m.getName()), cacheManagers.get(0), null);
      return jCacheManager.getCache("default");
   }

   @Override
   public Cache getCache2(Method m) {
      JCacheManager jCacheManager = new JCacheManager(URI.create(m.getName()), cacheManagers.get(1), null);
      return jCacheManager.getCache("default");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultClusteredCacheConfig1 = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(2, "default", defaultClusteredCacheConfig1);
      waitForClusterToForm("default");
   }

}
