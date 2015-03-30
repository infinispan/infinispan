package org.infinispan.jcache;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.util.JCacheTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.cache.Cache;
import javax.cache.Caching;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.jcache.util.JCacheTestingUtil.createCacheWithProperties;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.JCacheTwoCachesExpirationTest", groups = "functional")
@CleanupAfterMethod
public class JCacheTwoCachesExpirationTest extends AbstractTwoCachesExpirationTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private Cache cache1;
   private Cache cache2;
   private ClassLoader testSpecificClassLoader;

   // Enable once implemented
   @Test(enabled = false, description = "ISPN-5482")
   @Override
   public void testExpiration(Method m) {
      super.testExpiration(m);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, "expiry", getExpiryCacheConfig());

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(cacheManagers.get(0));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(cacheManagers.get(1));
      testSpecificClassLoader = new JCacheTestingUtil.TestClassLoader(JCacheTwoCachesExpirationTest.class.getClassLoader());

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer1.getHost() + ":" + hotRodServer1.getPort());
      cache1 = createCacheWithProperties(Caching.getCachingProvider(testSpecificClassLoader), JCacheTwoCachesExpirationTest.class, "expiry", properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer2.getHost() + ":" + hotRodServer2.getPort());
      cache2 = createCacheWithProperties(Caching.getCachingProvider(testSpecificClassLoader), JCacheTwoCachesExpirationTest.class, "expiry", properties);

      waitForClusterToForm("expiry");
   }

   protected static org.infinispan.configuration.cache.ConfigurationBuilder getExpiryCacheConfig() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.expiration().lifespan(2000);
      return hotRodCacheConfiguration(builder);
   }

   @AfterClass
   @Override
   protected void destroy() {
      super.destroy();
      killServers(hotRodServer1, hotRodServer2);
      Caching.getCachingProvider(testSpecificClassLoader).close();
   }

   @Override
   public Cache getCache1(Method m) {
      return cache1;
   }

   @Override
   public Cache getCache2(Method m) {
      return cache2;
   }
}
