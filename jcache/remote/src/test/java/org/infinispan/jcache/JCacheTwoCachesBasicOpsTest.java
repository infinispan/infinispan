package org.infinispan.jcache;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
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
@Test(testName = "org.infinispan.jcache.JCacheTwoCachesBasicOpsTest", groups = "functional")
@CleanupAfterMethod
public class JCacheTwoCachesBasicOpsTest extends AbstractTwoCachesBasicOpsTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private Cache cache1;
   private Cache cache2;
   private ClassLoader testSpecificClassLoader;

   @Override
   protected void createCacheManagers() throws Throwable {

      createClusteredCaches(2, "default", hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC)));

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(cacheManagers.get(0));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(cacheManagers.get(1));
      testSpecificClassLoader = new JCacheTestingUtil.TestClassLoader(JCacheTwoCachesBasicOpsTest.class.getClassLoader());

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer1.getHost() + ":" + hotRodServer1.getPort());
      cache1 = createCacheWithProperties(Caching.getCachingProvider(testSpecificClassLoader), JCacheTwoCachesBasicOpsTest.class, "default", properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer2.getHost() + ":" + hotRodServer2.getPort());
      cache2 = createCacheWithProperties(Caching.getCachingProvider(testSpecificClassLoader), JCacheTwoCachesBasicOpsTest.class, "default", properties);

      waitForClusterToForm("default");
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
