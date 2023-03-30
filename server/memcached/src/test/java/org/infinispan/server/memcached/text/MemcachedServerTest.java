package org.infinispan.server.memcached.text;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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

   public void testValidateDefaultConfiguration() {
      Stoppable.useCacheManager(TestCacheManagerFactory.createCacheManager(), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               assertEquals(ms.getHost(), "127.0.0.1");
               assertEquals((int) ms.getPort(), 11211);
            }));
   }

   public void testNoDefaultConfigurationLocal() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(global.build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               assertEquals(CacheMode.LOCAL, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testNoDefaultConfigurationClustered() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(global.build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
               assertEquals(CacheMode.REPL_SYNC, ms.getCache().getCacheConfiguration().clustering().cacheMode());
            }));
   }

   public void testProtocolDetectionBinary() {
      testProtocolDetection(ConnectionFactoryBuilder.Protocol.BINARY);
   }

   public void testProtocolDetectionText() {
      testProtocolDetection(ConnectionFactoryBuilder.Protocol.TEXT);
   }

   private void testProtocolDetection(ConnectionFactoryBuilder.Protocol protocol) {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      Stoppable.useCacheManager(new DefaultCacheManager(global.build()), cm ->
            Stoppable.useServer(new MemcachedServer(), ms -> {
               ms.start(new MemcachedServerConfigurationBuilder().build(), cm);
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
