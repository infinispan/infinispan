package org.infinispan.server.memcached.text;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

import net.spy.memcached.ClientMode;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

/**
 * Memcached server unit test.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.text.MemcachedServerTest")
public class MemcachedServerTest extends AbstractInfinispanTest {

   private static EmbeddedCacheManager createCacheManager(Method method, ConfigurationBuilder configuration) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
      addGlobalState(globalBuilder, method);
      return TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, configuration);
   }

   private static GlobalConfigurationBuilder addGlobalState(GlobalConfigurationBuilder globalBuilder, Method method) {
      String stateDirectory = tmpDirectory(MemcachedServerTest.class.getSimpleName() + File.separator + method.getName());
      Util.recursiveFileRemove(stateDirectory);
      globalBuilder.globalState().enable()
            .persistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .sharedPersistentLocation(stateDirectory);
      return globalBuilder;
   }

   public void testValidateDefaultConfiguration(Method m) {
      Stoppable.useCacheManager(createCacheManager(m, null), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals(ms.getHost(), "127.0.0.1");
               assertEquals((int) ms.getPort(), 11211);
            }));
   }

   public void testNoDefaultConfigurationLocal(Method m) {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(addGlobalState(global, m).build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals(CacheMode.LOCAL, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testNoDefaultConfigurationClustered(Method m) {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(addGlobalState(global, m).build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               assertEquals(CacheMode.REPL_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testProtocolDetectionBinary(Method m) {
      testProtocolDetection(m, ConnectionFactoryBuilder.Protocol.BINARY);
   }

   public void testProtocolDetectionText(Method m) {
      testProtocolDetection(m, ConnectionFactoryBuilder.Protocol.TEXT);
   }

   private void testProtocolDetection(Method m, ConnectionFactoryBuilder.Protocol protocol) {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(addGlobalState(global, m).build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               ms.postStart();
               await(ms.initializeDefaultCache());
               ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder().setProtocol(protocol).setOpTimeout(5).setClientMode(ClientMode.Static);
               MemcachedClient client = null;
               try {
                  client = new MemcachedClient(builder.build(), Collections.singletonList(new InetSocketAddress(ms.getHost(), ms.getPort())));
                  client.getVersions();
               } catch (IOException e) {
                  throw new RuntimeException(e);
               } finally {
                  if (client != null) {
                     client.shutdown();
                  }
               }
            }));
   }
}
