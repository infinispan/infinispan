package org.infinispan.jcache;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.jcache.util.JCacheTestingUtil.createCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.lang.reflect.Method;
import java.util.Properties;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.jcache.util.JCacheTestingUtil;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.JCacheRemoteTwoCachesBasicOpsTest", groups = "functional")
public class JCacheRemoteTwoCachesBasicOpsTest extends AbstractTwoCachesBasicOpsTest {

   public static final String CACHE_NAME = "jcache-remote-cache";
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private CacheManager cm1;
   private CacheManager cm2;
   private ClassLoader testSpecificClassLoader;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, CACHE_NAME, hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC)));

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(cacheManagers.get(0));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(cacheManagers.get(1));
      testSpecificClassLoader = new JCacheTestingUtil.TestClassLoader(JCacheRemoteTwoCachesBasicOpsTest.class.getClassLoader());

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer1.getHost() + ":" + hotRodServer1.getPort());
      cm1 = createCacheManager(Caching.getCachingProvider(testSpecificClassLoader), JCacheRemoteTwoCachesBasicOpsTest.class, CACHE_NAME, properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer2.getHost() + ":" + hotRodServer2.getPort());
      cm2 = createCacheManager(Caching.getCachingProvider(testSpecificClassLoader), JCacheRemoteTwoCachesBasicOpsTest.class, CACHE_NAME, properties);

      waitForClusterToForm(CACHE_NAME);
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
      return cm1.getCache(CACHE_NAME);
   }

   @Override
   public Cache getCache2(Method m) {
      return cm2.getCache(CACHE_NAME);
   }
}
