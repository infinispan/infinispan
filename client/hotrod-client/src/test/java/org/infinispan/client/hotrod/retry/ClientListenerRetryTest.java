package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.extractField;
import static org.infinispan.test.TestingUtil.replaceField;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec25;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;

/**
 * Tests for a client with a listener when connection to the server drops.
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ClientListenerRetryTest")
@SuppressWarnings("unused")
public class ClientListenerRetryTest extends MultiHotRodServersTest {

   private AtomicInteger counter = new AtomicInteger(0);
   private FailureInducingCodec failureInducingCodec = new FailureInducingCodec();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
      clients.forEach(rcm -> {
         Object listenerNotifier = extractField(rcm, "listenerNotifier");
         replaceField(failureInducingCodec, "codec", listenerNotifier, ClientListenerNotifier.class);
      });
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      return hotRodCacheConfiguration(builder);
   }

   @Test
   public void testConnectionDrop() throws Exception {
      RemoteCache<Integer, String> remoteCache = client(0).getCache();
      Listener listener = new Listener();
      remoteCache.addClientListener(listener);

      assertListenerActive(remoteCache, listener);

      failureInducingCodec.induceFailure();

      addItems(remoteCache, 10);

      failureInducingCodec.resetFailure();

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

   private static class FailureInducingCodec extends Codec25 {
      private volatile boolean failure;
      private final IOException failWith = new IOException("Connection reset by peer");

      @Override
      public ClientEvent readEvent(ByteBuf buf, byte[] expectedListenerId, Marshaller marshaller, List<String> whitelist, SocketAddress serverAddress) {
         if (failure) {
            throw new TransportException(failWith, serverAddress);
         }
         return super.readEvent(buf, expectedListenerId, marshaller, whitelist, serverAddress);
      }

      private void induceFailure() {
         failure = true;
      }

      private void resetFailure() {
         failure = false;
      }
   }

}
