package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.jboss.byteman.contrib.bmunit.BMNGListener;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

/**
 * Tests for a client with a listener when connection to the server drops.
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ClientListenerRetryTest")
@Listeners(BMNGListener.class)
@SuppressWarnings("unused")
public class ClientListenerRetryTest extends MultiHotRodServersTest {

   static volatile boolean induceFailure;
   static volatile boolean handled;

   static final Throwable FAIL_WITH = new IOException("Connection reset by peer");

   private AtomicInteger counter = new AtomicInteger(0);

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      return hotRodCacheConfiguration(builder);
   }

   @Test
   @BMRule(name = "Induce failure during event receiving",
         isInterface = true,
         targetClass = "org.infinispan.client.hotrod.impl.protocol.Codec",
         targetMethod = "readEvent",
         binding = "failWithException:Throwable = org.infinispan.client.hotrod.retry.ClientListenerRetryTest.FAIL_WITH;" +
               "induceFailure:boolean = org.infinispan.client.hotrod.retry.ClientListenerRetryTest.induceFailure",
         condition = "induceFailure",
         action = "org.infinispan.client.hotrod.retry.ClientListenerRetryTest.handled = true; " +
               "throw new org.infinispan.client.hotrod.exceptions.TransportException(failWithException, $1.getRemoteSocketAddress())")
   public void testConnectionDrop() throws Exception {
      RemoteCache<Integer, String> remoteCache = client(0).getCache();
      Listener listener = new Listener();
      remoteCache.addClientListener(listener);

      assertListenerActive(remoteCache, listener);

      induceFailure = true;

      addItems(remoteCache, 10);

      induceFailure = false;

      assertListenerActive(remoteCache, listener);

      assertTrue(handled);
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
