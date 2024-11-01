package org.infinispan.client.hotrod.retry;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.impl.AbstractClientEvent;
import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.protocol.Codec41;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;

/**
 * Tests for a client with a listener when connection to the server drops.
 */
@Test(groups = "functional", testName = "client.hotrod.retry.ClientListenerRetryTest")
@SuppressWarnings("unused")
public class ClientListenerRetryTest extends MultiHotRodServersTest {

   private final AtomicInteger counter = new AtomicInteger(0);
   private final FailureInducingCodec failureInducingCodec = new FailureInducingCodec();

   @Override
   protected RemoteCacheManager createClient(int i) {
      AtomicReference<HeaderDecoder> headerDecoderRef = new AtomicReference<>();
      InternalRemoteCacheManager ircm = new InternalRemoteCacheManager(testReplay, createHotRodClientConfigurationBuilder(server(i)).build()) {
         @Override
         protected Consumer<ChannelPipeline> pipelineWrapper() {
            return cp -> {
               super.pipelineWrapper().accept(cp);
               TestingUtil.replaceField(failureInducingCodec, "codec", cp.get(HeaderDecoder.class), HeaderDecoder.class);
            };
         }
      };
      return ircm;
   }

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

   private static class FailureInducingCodec extends Codec41 {
      private volatile boolean failure;
      private final IOException failWith = new IOException("Connection reset by peer");

      @Override
      public AbstractClientEvent readCacheEvent(ByteBuf buf, long messageId, Function<byte[], DataFormat> listenerDataFormat, short eventTypeId, ClassAllowList allowList, SocketAddress serverAddress) {
         if (failure) {
            throw new TransportException(failWith, serverAddress);
         }
         return super.readCacheEvent(buf, messageId, listenerDataFormat, eventTypeId, allowList, serverAddress);
      }

      private void induceFailure() {
         failure = true;
      }

      private void resetFailure() {
         failure = false;
      }
   }
}
