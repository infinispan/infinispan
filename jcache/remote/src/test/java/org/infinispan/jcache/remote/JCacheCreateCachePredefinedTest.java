package org.infinispan.jcache.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.net.URI;
import java.util.Properties;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jcache.remote.JCacheCreateCachePredefinedTest")
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

   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      CachingProvider provider = Caching.getCachingProvider();
      URI name = URI.create(JCacheCreateCachePredefinedTest.class.getName());
      ClassLoader cl = JCacheCreateCachePredefinedTest.class.getClassLoader();

      Properties properties = new Properties();
      properties.put("infinispan.client.hotrod.server_list", hotrodServer.getHost() + ":" + hotrodServer.getPort());
      jcacheManager = provider.getCacheManager(name, cl, properties);
   }

   @Override
   protected void teardown() {
      jcacheManager.close();

      super.teardown();
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*ISPN021015:.*")
   public void testCreateCachePredefinedTouched() {
      jcacheManager.getCache(CACHE_NAME_TOUCHED); // touch it
      jcacheManager.createCache(CACHE_NAME_TOUCHED, new MutableConfiguration<>());
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*ISPN021052:.*", enabled = false, description = "ISPN-9237")
   public void testCreateCachePredefinedUntouched() {
         jcacheManager.createCache(CACHE_NAME_UNTOUCHED, new MutableConfiguration<>());
   }

}
