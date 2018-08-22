package org.infinispan.jcache.remote;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.jcache.util.JCacheTestingUtil.createCacheWithProperties;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.replaceComponent;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.cache.Cache;
import javax.cache.Caching;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jcache.AbstractTwoCachesExpirationTest;
import org.infinispan.jcache.util.JCacheTestingUtil;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Matej Cimbora
 */
@Test(testName = "org.infinispan.jcache.remote.JCacheTwoCachesExpirationTest", groups = "functional")
@CleanupAfterMethod
public class JCacheTwoCachesExpirationTest extends AbstractTwoCachesExpirationTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private Cache cache1;
   private Cache cache2;
   private ClassLoader testSpecificClassLoader;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, "expiry", getExpiryCacheConfig());
      cacheManagers.forEach(cm -> replaceComponent(cm, TimeService.class, controlledTimeService, true));

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(
            cacheManagers.get(0),
            new HotRodServerConfigurationBuilder().adminOperationsHandler(new EmbeddedServerAdminOperationHandler()));
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(
            cacheManagers.get(1),
            new HotRodServerConfigurationBuilder().adminOperationsHandler(new EmbeddedServerAdminOperationHandler()));
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
      builder.expiration().lifespan(EXPIRATION_TIMEOUT).wakeUpInterval(100, TimeUnit.MILLISECONDS);
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
