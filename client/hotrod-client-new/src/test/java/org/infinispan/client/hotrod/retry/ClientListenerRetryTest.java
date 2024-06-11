package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests for a client with a listener when connection to the server drops.
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ClientListenerRetryTest")
@SuppressWarnings("unused")
public class ClientListenerRetryTest extends MultiHotRodServersTest {

   private final AtomicInteger counter = new AtomicInteger(0);

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      return hotRodCacheConfiguration(builder);
   }

   @Test
   public void testConnectionDrop() {
      RemoteCache<Integer, String> remoteCache = client(0).getCache();
      Listener listener = new Listener();
      remoteCache.addClientListener(listener);

      assertListenerActive(remoteCache, listener);

      // TODO: need to update to throw TransportException
//      failureInducingCodec.induceFailure();

      addItems(remoteCache, 10);

//      failureInducingCodec.resetFailure();

      assertListenerActive(remoteCache, listener);
   }

   private void addItems(RemoteCache<Integer, String> cache, int items) {
      IntStream.range(0, items).forEach(i -> cache.put(counter.incrementAndGet(), "value"));
   }

   private void assertListenerActive(RemoteCache<Integer, String> cache, Listener listener) {
      int received = listener.getReceived();
      eventually(() -> {
         cache.put(counter.incrementAndGet(), "value");
         return listener.getReceived() > received;
      });
   }

   @ClientListener
   private static class Listener {

      private final AtomicInteger count = new AtomicInteger(0);

      @ClientCacheEntryCreated
      public void handleCreatedEvent(ClientCacheEntryCreatedEvent<?> e) {
         count.incrementAndGet();
      }

      int getReceived() {
         return count.intValue();
      }

   }

   @Override
   protected int maxRetries() {
      return 10;
   }
}
