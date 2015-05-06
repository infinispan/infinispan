package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.findServerAndKill;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterFailoverEventsTest")
public class ClientClusterFailoverEventsTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      // Empty
   }

   public void testEventReplayWithAndWithoutStateAfterFailover() {
      ConfigurationBuilder base = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      //base.clustering().hash().numOwners(1);
      ConfigurationBuilder builder = hotRodCacheConfiguration(base);
      createHotRodServers(2, builder);
      try {
         final Integer key0 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
         final Integer key11 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
         final Integer key21 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
         final Integer key31 = HotRodClientTestingUtil.getIntKeyForServer(server(1));

         org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
               new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
         HotRodServer server = server(0);
         clientBuilder.addServers(server.getHost() + ":" + server.getPort());
         clientBuilder.balancingStrategy(StickyServerLoadBalancingStrategy.class);
         RemoteCacheManager newClient = new RemoteCacheManager(clientBuilder.build());
         try {
            WithStateEventLogListener<Integer> statefulListener = new WithStateEventLogListener<>();
            EventLogListener<Integer> statelessListener = new EventLogListener<>();
            FailoverEventLogListener<Integer> failoverListener = new FailoverEventLogListener<>();
            RemoteCache<Integer, String> c = newClient.getCache();
            c.addClientListener(statelessListener);
            c.addClientListener(statefulListener);
            c.addClientListener(failoverListener);
            c.put(key0, "zero");
            statefulListener.expectOnlyCreatedEvent(key0, cache(0));
            statelessListener.expectOnlyCreatedEvent(key0, cache(0));
            failoverListener.expectOnlyCreatedEvent(key0, cache(0));
            c.put(key11, "one");
            statefulListener.expectOnlyCreatedEvent(key11, cache(0));
            statelessListener.expectOnlyCreatedEvent(key11, cache(0));
            failoverListener.expectOnlyCreatedEvent(key11, cache(0));
            findServerAndKill(newClient, servers, cacheManagers);
            c.put(key21, "two");
            // Failover expectations
            statelessListener.expectNoEvents();
            statefulListener.expectFailoverEvent();
            failoverListener.expectFailoverEvent();
            // State expectations
            statelessListener.expectNoEvents();
            failoverListener.expectNoEvents();
            statefulListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, key0, key11, key21);
            c.put(key31, "three");
            statefulListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, key31);
            c.remove(key11);
            c.remove(key21);
            c.remove(key31);
         } finally {
            killRemoteCacheManager(newClient);
         }
      } finally {
         destroy();
      }

   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K> extends FailoverEventLogListener<K> {}

}
