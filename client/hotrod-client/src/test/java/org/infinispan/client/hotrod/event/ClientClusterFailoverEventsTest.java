package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.findServerAndKill;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClientClusterFailoverEventsTest")
public class ClientClusterFailoverEventsTest extends MultiHotRodServersTest {

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Empty
   }

   private void injectTimeServices() {
      long now = System.currentTimeMillis();
      ts0 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(server(0).getCacheManager(), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(server(1).getCacheManager(), TimeService.class, ts1, true);
   }

   public void testEventReplayWithAndWithoutStateAfterFailover() {
      ConfigurationBuilder base = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      //base.clustering().hash().numOwners(1);
      ConfigurationBuilder builder = hotRodCacheConfiguration(base);
      createHotRodServers(2, builder);
      injectTimeServices();
      try {
         final Integer key00 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
         final Integer key10 = HotRodClientTestingUtil.getIntKeyForServer(server(0));
         final Integer key11 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
         final Integer key21 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
         final Integer key31 = HotRodClientTestingUtil.getIntKeyForServer(server(1));
         final Integer key41 = HotRodClientTestingUtil.getIntKeyForServer(server(1));

         org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
               new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
         HotRodServer server = server(0);
         clientBuilder.addServers(server.getHost() + ":" + server.getPort());
         clientBuilder.balancingStrategy(StickyServerLoadBalancingStrategy.class);
         RemoteCacheManager newClient = new RemoteCacheManager(clientBuilder.build());
         try {
            RemoteCache<Integer, String> c = newClient.getCache();
            WithStateEventLogListener<Integer> statefulListener = new WithStateEventLogListener<>(c);
            EventLogListener<Integer> statelessListener = new EventLogListener<>(c);
            FailoverEventLogListener<Integer> failoverListener = new FailoverEventLogListener<>(c);
            c.addClientListener(statelessListener);
            c.addClientListener(statefulListener);
            c.addClientListener(failoverListener);
            c.put(key00, "zero");
            statefulListener.expectOnlyCreatedEvent(key00);
            statelessListener.expectOnlyCreatedEvent(key00);
            failoverListener.expectOnlyCreatedEvent(key00);
            c.put(key10, "one", 1000, TimeUnit.MILLISECONDS);
            statefulListener.expectOnlyCreatedEvent(key10);
            statelessListener.expectOnlyCreatedEvent(key10);
            failoverListener.expectOnlyCreatedEvent(key10);
            c.put(key11, "two");
            statefulListener.expectOnlyCreatedEvent(key11);
            statelessListener.expectOnlyCreatedEvent(key11);
            failoverListener.expectOnlyCreatedEvent(key11);
            c.put(key41, "three", 1000, TimeUnit.MILLISECONDS);
            statefulListener.expectOnlyCreatedEvent(key41);
            statelessListener.expectOnlyCreatedEvent(key41);
            failoverListener.expectOnlyCreatedEvent(key41);
            ts0.advance(1001);
            ts1.advance(1001);
            findServerAndKill(newClient, servers, cacheManagers);
            // The failover is asynchronous, triggered by closing the channels. If we did an operation right
            // now we could get this event.
            // c.put(key21, "four");
            // Failover expectations
            statelessListener.expectNoEvents();
            statefulListener.expectFailoverEvent();
            failoverListener.expectFailoverEvent();
            // State expectations
            statelessListener.expectNoEvents();
            failoverListener.expectNoEvents();
            //we should receive CLIENT_CACHE_ENTRY_CREATED only for entries that did not expire
            statefulListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, key00, key11);
            //now there should be no events for key10 and key41 as they expired
            statefulListener.expectNoEvents();
            c.put(key31, "five");
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
   public static class WithStateEventLogListener<K> extends FailoverEventLogListener<K> {
      public WithStateEventLogListener(RemoteCache<K, ?> remote) {
         super(remote);
      }
   }

}
