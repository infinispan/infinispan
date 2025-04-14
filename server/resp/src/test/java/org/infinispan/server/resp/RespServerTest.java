package org.infinispan.server.resp;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.dataconversion.MediaType;
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
               assertEquals("127.0.0.1", ms.getHost());
               assertEquals(RespServerConfiguration.DEFAULT_RESP_CACHE, ms.getCache().getName());
            }));
   }

   public void testExpiration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM);
      config.expiration().lifespan(10);
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(config), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).defaultCacheName(cm.getCacheManagerConfiguration().defaultCacheName().get()).build(), cm);
               assertEquals(10, ms.getCache().getCacheConfiguration().expiration().lifespan());
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
               assertEquals(CacheMode.DIST_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }
}
