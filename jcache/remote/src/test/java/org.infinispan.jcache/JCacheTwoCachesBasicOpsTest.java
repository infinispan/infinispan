package org.infinispan.jcache;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jcache.AbstractTwoCachesBasicOpsTest;
import org.infinispan.jcache.util.JCacheTestingUtil.TestClassLoader;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.util.Properties;

import static org.infinispan.jcache.util.JCacheTestingUtil.createCacheWithProperties;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Matej Cimbora
 */
public class JCacheTwoCachesBasicOpsTest extends AbstractTwoCachesBasicOpsTest {

   private static HotRodServer hotRodServer1;
   private static HotRodServer hotRodServer2;
   private static CachingProvider cachingProvider;
   private static Cache cache1;
   private static Cache cache2;
   private static Cache expiryCache1;
   private static Cache expiryCache2;

   @BeforeClass
   public static void init() {
      GlobalConfiguration gc1 = new GlobalConfigurationBuilder().transport().defaultTransport().globalJmxStatistics().allowDuplicateDomains(true).build();
      GlobalConfiguration gc2 = new GlobalConfigurationBuilder().transport().defaultTransport().globalJmxStatistics().allowDuplicateDomains(true).build();
      Configuration c1 = new org.infinispan.configuration.cache.ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      Configuration c2 = new org.infinispan.configuration.cache.ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();

      EmbeddedCacheManager cm1 = new DefaultCacheManager(gc1, c1);
      cm1.defineConfiguration("remote", getCacheConfig().build());
      cm1.defineConfiguration("expiryCache", getExpiryCacheConfig().build());
      EmbeddedCacheManager cm2 = new DefaultCacheManager(gc2, c2);
      cm2.defineConfiguration("remote", getCacheConfig().build());
      cm2.defineConfiguration("expiryCache", getExpiryCacheConfig().build());

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(cm1);
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(cm2);

      cachingProvider = Caching.getCachingProvider(new TestClassLoader(Thread.currentThread().getContextClassLoader()));

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer1.getHost() + ":" + hotRodServer1.getPort());
      cache1 = createCacheWithProperties(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "remote", properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer2.getHost() + ":" + hotRodServer2.getPort());
      cache2 = createCacheWithProperties(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "remote", properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer1.getHost() + ":" + hotRodServer1.getPort());
      expiryCache1 = createCacheWithProperties(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "expiryCache", properties);

      properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotRodServer2.getHost() + ":" + hotRodServer2.getPort());
      expiryCache2 = createCacheWithProperties(cachingProvider, JCacheTwoCachesBasicOpsTest.class, "expiryCache", properties);
   }

   @AfterClass
   public static void destroy() {
      cache1.close();
      cache2.close();
      expiryCache1.close();
      expiryCache2.close();
      cachingProvider.close();
      HotRodClientTestingUtil.killServers(hotRodServer1, hotRodServer2);
   }

   @After
   public void clearCaches() {
      cache1.clear();
      cache2.clear();
      expiryCache1.clear();
      expiryCache2.clear();
   }

   @Override
   public void testExpiration() {
      // TODO enable when working correctly
   }

   @Override
   public Cache getCache1() {
      return cache1;
   }

   @Override
   public Cache getCache2() {
      return cache2;
   }

   @Override
   public Cache getExpiryCache1() {
      return expiryCache1;
   }

   @Override
   public Cache getExpiryCache2() {
      return expiryCache2;
   }

   protected static org.infinispan.configuration.cache.ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      return hotRodCacheConfiguration(builder);
   }

   protected static org.infinispan.configuration.cache.ConfigurationBuilder getExpiryCacheConfig() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).expiration().lifespan(2000);
      return hotRodCacheConfiguration(builder);
   }

}
