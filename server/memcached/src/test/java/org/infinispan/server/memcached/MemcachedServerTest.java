package org.infinispan.server.memcached;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Memcached server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedServerTest")
public class MemcachedServerTest extends AbstractInfinispanTest {

   public void testValidateDefaultConfiguration() {
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               assertEquals(ms.getHost(), "127.0.0.1");
               assertEquals((int) ms.getPort(), 11211);
            }));
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testValidateInvalidExpiration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.expiration().lifespan(10);
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(config), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().defaultCacheName("memcachedCache").build(), cm);
               fail("Server should not start when expiration is enabled");
            }));
   }

   public void testNoDefaultConfigurationLocal() {
      Stoppable.useCacheManager(new DefaultCacheManager(), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               assertEquals(CacheMode.LOCAL, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testNoDefaultConfigurationClustered() {
      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      TestCacheManagerFactory.amendGlobalConfiguration(gc, new TransportFlags());
      Stoppable.useCacheManager(new DefaultCacheManager(gc.build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               assertEquals(CacheMode.REPL_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }
}
