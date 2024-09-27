package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.BasicClientIntelligenceTest")
public class BasicClientIntelligenceTest extends MultiHotRodServersTest {
   private final ConfigurationBuilder builder = hotRodCacheConfiguration(
         getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServersWithoutClients(2, builder);
   }

   public void testOneServerDiedAndComesBack() {
      int initialPort = server(1).getPort();

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      // Retry after every half second to make test faster
      clientBuilder.basicFailedTimeout(500);
      clientBuilder.addServers(HotRodClientTestingUtil.getServersString(server(0), server(1)));
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            RemoteCache<Object, Object> cache = rcm.getCache();
            OperationDispatcher dispatcher = rcm.getOperationDispatcher();
            assertFalse(cache.containsKey("k"));
            killServer(1);
            eventuallyEquals(1, () -> {
               assertFalse(cache.containsKey("k"));
               return dispatcher.getConnectionFailedServers().size();
            });

            addHotRodServer(builder, initialPort);

            eventuallyEquals(0, () -> {
               assertFalse(cache.containsKey("k"));
               return dispatcher.getConnectionFailedServers().size();
            });
         }
      });
   }

   public void testBasicHasSingleServerThatDied() {
      int initialPort = server(1).getPort();

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.basicFailedTimeout(500);
      clientBuilder.addServers(HotRodClientTestingUtil.getServersString(server(1)));
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);

      withRemoteCacheManager(new RemoteCacheManagerCallable(
            new InternalRemoteCacheManager(clientBuilder.build())) {
         @Override
         public void call() {
            RemoteCache<Object, Object> cache = rcm.getCache();
            OperationDispatcher dispatcher = rcm.getOperationDispatcher();
            assertFalse(cache.containsKey("k"));
            killServer(1);
            for (int i = 0; i < 10; i++) {
               Exceptions.expectException(TransportException.class, () -> cache.containsKey("k"));
            }
            eventuallyEquals(1, () -> dispatcher.getConnectionFailedServers().size());

            addHotRodServer(builder, initialPort);

            eventuallyEquals(0, () -> {
               assertFalse(cache.containsKey("k"));
               return dispatcher.getConnectionFailedServers().size();
            });
         }
      });
   }
}
