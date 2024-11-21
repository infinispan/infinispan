package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec25;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.configuration.ClassAllowList;
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

   private final AtomicInteger counter = new AtomicInteger(0);
   private final FailureInducingCodec failureInducingCodec = new FailureInducingCodec();

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(2, getCacheConfiguration());
      clients.forEach(rcm -> rcm.getChannelFactory().setNegotiatedCodec(failureInducingCodec));
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      // disable protocol negotiation since we want to use FailureInducingCodec
      return super.createHotRodClientConfigurationBuilder(host, serverPort).version(ProtocolVersion.PROTOCOL_VERSION_25).socketTimeout(60_000);
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
      public AbstractClientEvent readCacheEvent(ByteBuf buf, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress) {
         if (failure) {
            throw new TransportException(failWith, serverAddress);
         }
         return super.readCacheEvent(buf, listenerDataFormat, eventTypeId, allowList, serverAddress);
      }

      private void induceFailure() {
         failure = true;
      }

      private void resetFailure() {
         failure = false;
      }
   }

}
