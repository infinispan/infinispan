package org.infinispan.server.resp;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Resp server unit test.
 *
 * @author William Burns
 * @since 14.0
 */
@Test(groups = "functional", testName = "server.resp.RespServerTest")
public class RespServerTest extends AbstractInfinispanTest {

   public void testValidateDefaultConfiguration() {
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               assertEquals(ms.getHost(), "127.0.0.1");
               assertEquals(ms.getCache().getName(), RespServerConfiguration.DEFAULT_RESP_CACHE);
            }));
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testValidateInvalidExpiration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.expiration().lifespan(10);
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(config), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().defaultCacheName(cm.getCacheManagerConfiguration().defaultCacheName().get()).build(), cm);
               fail("Server should not start when expiration is enabled");
            }));
   }

   public void testNoDefaultConfigurationLocal() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(global.build()), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               assertEquals(CacheMode.LOCAL, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testNoDefaultConfigurationClustered() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(global.build()), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               assertEquals(CacheMode.REPL_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }
}
