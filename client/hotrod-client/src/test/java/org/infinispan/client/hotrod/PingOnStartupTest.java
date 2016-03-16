package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Tests ping-on-startup logic whose objective is to retrieve the Hot Rod
 * server topology before a client executes an operation against the server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.PingOnStartupTest")
public class PingOnStartupTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      createHotRodServers(2, builder);
   }

   public void testTopologyFetched() {
      HotRodServer hotRodServer2 = server(1);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .addServers("localhost:" + hotRodServer2.getPort() + ";localhost:" + hotRodServer2.getPort());
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            TcpTransportFactory tcpTransportFactory =
                  (TcpTransportFactory) ((InternalRemoteCacheManager) rcm).getTransportFactory();
            for (int i = 0; i < 10; i++) {
               if (tcpTransportFactory.getServers().size() == 1) {
                  TestingUtil.sleepThread(1000);
               } else {
                  break;
               }
            }
            assertEquals(2, tcpTransportFactory.getServers().size());
         }
      });
   }

   public void testGetCacheWithPingOnStartupDisabledMultipleNodes() {
      HotRodServer hotRodServer2 = server(1);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .addServers("boomoo:12345;localhost:" + hotRodServer2.getPort());
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            RemoteCache<Object, Object> cache = rcm.getCache();
            assertFalse(cache.containsKey("k"));
         }
      });
   }

   public void testGetCacheWorksIfNodeDown() {
      HotRodServer hotRodServer2 = server(1);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .addServers("boomoo:12345;localhost:" + hotRodServer2.getPort());
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            rcm.getCache();
         }
      });
   }

   public void testGetCacheWorksIfNodeNotDown() {
      HotRodServer hotRodServer2 = server(1);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .addServers("localhost:" + hotRodServer2.getPort());
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new RemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            rcm.getCache();
         }
      });
   }

}
