package org.infinispan.server.resp;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * Resp server unit test.
 *
 * @author William Burns
 * @since 14.0
 */
@Test(groups = "functional", testName = "server.resp.RespServerTest")
public class RespServerTest extends AbstractInfinispanTest {
   private GlobalConfigurationBuilder gcb;
   private String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), this.getClass().getSimpleName());

   @BeforeMethod(alwaysRun = true)
   public void init() {
      gcb = new GlobalConfigurationBuilder();
      Util.recursiveFileRemove(stateDirectory);
      gcb.globalState().enable().persistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      gcb.globalState().sharedPersistentLocation(stateDirectory);
   }

   public void testValidateDefaultConfiguration() {
      ConfigurationBuilder cb = new ConfigurationBuilder();

      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(gcb, cb), cm ->
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
      Stoppable.useCacheManager(new DefaultCacheManager(gcb.build()), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               assertEquals(CacheMode.LOCAL, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testNoDefaultConfigurationClustered() {
      gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.globalState().enable().persistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      gcb.globalState().sharedPersistentLocation(stateDirectory);
      Stoppable.useCacheManager(new DefaultCacheManager(gcb.build()), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               assertEquals(CacheMode.REPL_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }
}
