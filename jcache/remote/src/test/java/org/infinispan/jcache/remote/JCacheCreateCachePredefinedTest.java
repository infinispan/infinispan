package org.infinispan.jcache.remote;

import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import java.net.URI;
import java.util.Properties;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.*;

public class JCacheCreateCachePredefinedTest extends SingleHotRodServerTest {
   static String CACHE_NAME_UNTOUCHED = "jcache-remote-predefined-untouched";
   static String CACHE_NAME_TOUCHED = "jcache-remote-predefined-touched";
   private CacheManager jcacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager();
      cm.defineConfiguration(CACHE_NAME_UNTOUCHED, hotRodCacheConfiguration().build());
      cm.defineConfiguration(CACHE_NAME_TOUCHED, hotRodCacheConfiguration().build());
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      CachingProvider provider = Caching.getCachingProvider();
      URI name = URI.create(JCacheCreateCachePredefinedTest.class.getName());
      ClassLoader cl = JCacheCreateCachePredefinedTest.class.getClassLoader();

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotrodServer.getHost() + ":" + hotrodServer.getPort());
      properties.put("infinispan.jcache.remote.managed_access", "false");
      jcacheManager = provider.getCacheManager(name, cl, properties);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*ISPN021015:.*")
   public void testCreateCachePredefinedTouched() {
      jcacheManager.getCache(CACHE_NAME_TOUCHED); // touch it
      jcacheManager.createCache(CACHE_NAME_TOUCHED, new MutableConfiguration<>());
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*ISPN021052:.*")
   public void testCreateCachePredefinedUntouched() {
      jcacheManager.createCache(CACHE_NAME_UNTOUCHED, new MutableConfiguration<>());
   }

}
