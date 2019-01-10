package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

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
         HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotRodServer2);
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            ChannelFactory channelFactory = ((InternalRemoteCacheManager) rcm).getChannelFactory();
            for (int i = 0; i < 10; i++) {
               if (channelFactory.getServers().size() == 1) {
                  TestingUtil.sleepThread(1000);
               } else {
                  break;
               }
            }
            assertEquals(2, channelFactory.getServers().size());
         }
      });
   }

   public void testBasicIntelligence() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(server(0).getPort());
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            rcm.getCache();
            ChannelFactory channelFactory = ((InternalRemoteCacheManager) rcm).getChannelFactory();
            assertEquals(1, channelFactory.getServers().size());
         }
      });
   }

   public void testTopologyAwareIntelligence() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(server(0).getPort());
      clientBuilder.clientIntelligence(ClientIntelligence.TOPOLOGY_AWARE);
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            rcm.getCache();
            ChannelFactory channelFactory = ((InternalRemoteCacheManager) rcm).getChannelFactory();
            assertEquals(2, channelFactory.getServers().size());
         }
      });
   }

   public void testHashAwareIntelligence() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(server(0).getPort());
      clientBuilder.clientIntelligence(ClientIntelligence.HASH_DISTRIBUTION_AWARE);
      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            rcm.getCache();
            ChannelFactory channelFactory = ((InternalRemoteCacheManager) rcm).getChannelFactory();
            assertEquals(2, channelFactory.getServers().size());
         }
      });
   }

   public void testGetCacheWithPingOnStartupDisabledMultipleNodes() {
      HotRodServer hotRodServer2 = server(1);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
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
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
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
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
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
