package org.infinispan.jcache;

import static org.infinispan.test.TestingUtil.replaceComponent;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.cache.Cache;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.Test;

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
      defaultClusteredCacheConfig2.expiration().lifespan(EXPIRATION_TIMEOUT).wakeUpInterval(100, TimeUnit.MILLISECONDS);
      createClusteredCaches(2, "expiry", defaultClusteredCacheConfig2);
      cacheManagers.forEach(cm -> replaceComponent(cm, TimeService.class, controlledTimeService, true));
      waitForClusterToForm("expiry");
   }
}
