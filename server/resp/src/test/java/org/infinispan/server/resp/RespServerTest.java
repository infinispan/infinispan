package org.infinispan.server.resp;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.lang.reflect.Method;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Resp server unit test.
 *
 * @author William Burns
 * @since 14.0
 */
@Test(groups = "functional", testName = "server.resp.RespServerTest")
public class RespServerTest extends AbstractInfinispanTest {

   private static EmbeddedCacheManager createCacheManager(Method method, ConfigurationBuilder configuration) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
      addGlobalState(globalBuilder, method);
      return TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, configuration);
   }

   private static GlobalConfigurationBuilder addGlobalState(GlobalConfigurationBuilder globalBuilder, Method method) {
      String stateDirectory = tmpDirectory(RespServerTest.class.getSimpleName() + File.separator + method.getName());
      Util.recursiveFileRemove(stateDirectory);
      globalBuilder.globalState().enable()
            .persistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .sharedPersistentLocation(stateDirectory);
      return globalBuilder;
   }

   public void testValidateDefaultConfiguration(Method m) {
      Stoppable.useCacheManager(createCacheManager(m, null), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals("127.0.0.1", ms.getHost());
               assertEquals(RespServerConfiguration.DEFAULT_RESP_CACHE, ms.getCache().getName());
            }));
   }

   public void testExpiration(Method m) {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.encoding().key().mediaType(MediaType.APPLICATION_OCTET_STREAM);
      config.expiration().lifespan(10);
      Stoppable.useCacheManager(createCacheManager(m, config), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).defaultCacheName(cm.getCacheManagerConfiguration().defaultCacheName().get()).build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals(10, ms.getCache().getCacheConfiguration().expiration().lifespan());
            }));
   }

   public void testNoDefaultConfigurationLocal(Method m) {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(addGlobalState(global, m).build()), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals(CacheMode.LOCAL, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testNoDefaultConfigurationClustered(Method m) {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(addGlobalState(global, m).build()), cm ->
            Stoppable.useServer(new RespServer(), ms -> {
               ms.start(new RespServerConfigurationBuilder().port(0).build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals(CacheMode.DIST_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }
}
