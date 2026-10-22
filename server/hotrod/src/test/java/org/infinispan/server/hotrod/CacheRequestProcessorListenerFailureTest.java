package org.infinispan.server.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;

import org.infinispan.AdvancedCache;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.telemetry.impl.DisabledInfinispanTelemetry;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.netty.channel.embedded.EmbeddedChannel;

@Test(groups = "unit", testName = "server.hotrod.CacheRequestProcessorListenerFailureTest")
@SuppressWarnings("unchecked")
public class CacheRequestProcessorListenerFailureTest extends AbstractInfinispanTest {
   private static final byte[] LISTENER_ID = {1, 2, 3};

   private EmbeddedChannel channel;
   private AdvancedCache<byte[], byte[]> cache;
   private ClientListenerRegistry registry;
   private RecordingProcessor processor;
   private final RuntimeException failure = new IllegalStateException("Simulated listener registration failure");

   @BeforeMethod
   public void setup() {
      channel = new EmbeddedChannel();
      cache = mock(AdvancedCache.class, RETURNS_DEEP_STUBS);
      when(cache.removeListenerAsync(any())).thenReturn(CompletableFuture.completedFuture(null));
      var configuration = new HotRodServerConfigurationBuilder().build();
      registry = new ClientListenerRegistry(mock(EncoderRegistry.class), configuration, Runnable::run);
      HotRodServer server = mock(HotRodServer.class, RETURNS_DEEP_STUBS);
      when(server.getClientListenerRegistry()).thenReturn(registry);
      when(server.getConfiguration()).thenReturn(configuration);
      when(server.getCacheInfo(any())).thenReturn(mock(HotRodServer.ExtendedCacheInfo.class));
      when(server.cache(any(), any(), any())).thenReturn(cache);
      processor = new RecordingProcessor(channel, server);
   }

   @AfterMethod(alwaysRun = true)
   public void cleanup() {
      if (channel != null) {
         channel.finishAndReleaseAll();
      }
   }

   @DataProvider
   public Object[][] listenerModes() {
      return new Object[][]{{false}, {true}};
   }

   @Test(dataProvider = "listenerModes")
   public void testSynchronousFailureRemovesBloomFilter(boolean primary) {
      when(cache.addFilteredListenerAsync(any(), any(), any(), any())).thenThrow(failure);
      register(primary);

      processor.updateBloomFilter(header(primary), null, new byte[128]);
      assertEquals(OperationStatus.OperationNotExecuted, processor.status,
            "A failed registration must not leave a filter that accepts updates");
   }

   @Test(dataProvider = "listenerModes")
   public void testSynchronousFailureRemovesEventSender(boolean primary) {
      when(cache.addFilteredListenerAsync(any(), any(), any(), any())).thenThrow(failure);
      register(primary);

      assertFalse(registry.removeClientListener(LISTENER_ID, cache).toCompletableFuture().join(),
            "A failed registration must not leave an event sender in the registry");
   }

   @Test(dataProvider = "listenerModes")
   public void testSynchronousSetupFailureRemovesBloomFilter(boolean primary) {
      when(cache.getKeyDataConversion()).thenThrow(failure);
      register(primary);

      processor.updateBloomFilter(header(primary), null, new byte[128]);
      assertEquals(OperationStatus.OperationNotExecuted, processor.status);
      assertFalse(registry.removeClientListener(LISTENER_ID, cache).toCompletableFuture().join());
   }

   @Test(dataProvider = "listenerModes")
   public void testAsynchronousFailureRemovesRegistrationState(boolean primary) {
      when(cache.addFilteredListenerAsync(any(), any(), any(), any()))
            .thenReturn(CompletableFuture.failedFuture(failure));
      register(primary);

      processor.updateBloomFilter(header(primary), null, new byte[128]);
      assertEquals(OperationStatus.OperationNotExecuted, processor.status);
      assertFalse(registry.removeClientListener(LISTENER_ID, cache).toCompletableFuture().join());
   }

   @Test(dataProvider = "listenerModes")
   public void testLateFailurePreservesReplacementWithSameListenerId(boolean primary) {
      CompletableFuture<Void> first = new CompletableFuture<>();
      CompletableFuture<Void> replacement = new CompletableFuture<>();
      when(cache.addFilteredListenerAsync(any(), any(), any(), any())).thenReturn(first, replacement);
      startRegistration(primary);
      startRegistration(primary);
      first.completeExceptionally(failure);
      channel.runPendingTasks();
      assertSame(failure, processor.failure);

      processor.updateBloomFilter(header(primary), null, new byte[128]);
      assertEquals(OperationStatus.Success, processor.status,
            "Failure of the old registration must preserve the replacement filter");
      assertTrue(registry.removeClientListener(LISTENER_ID, cache).toCompletableFuture().join(),
            "Failure of the old registration must preserve the replacement event sender");
   }

   private void register(boolean primary) {
      startRegistration(primary);
      channel.runPendingTasks();
      assertSame(failure, processor.failure);
      assertEquals(OperationStatus.ServerError, processor.status);
   }

   private void startRegistration(boolean primary) {
      if (primary) {
         processor.addPrimaryClientListener(header(true), null, LISTENER_ID, 1024);
      } else {
         processor.addClientListener(header(false), null, LISTENER_ID, false,
               null, null, null, null, false, 14, 1024);
      }
   }

   private HotRodHeader header(boolean primary) {
      return new HotRodHeader(primary ? HotRodOperation.ADD_PRIMARY_NEAR_CACHE_LISTENER
            : HotRodOperation.ADD_BLOOM_FILTER_CLIENT_LISTENER, (byte) 42, 1, "cache", 0, (short) 1, 0,
            null, null, null);
   }

   private static class RecordingProcessor extends CacheRequestProcessor {
      private Throwable failure;
      private OperationStatus status;

      RecordingProcessor(EmbeddedChannel channel, HotRodServer server) {
         super(channel, Runnable::run, server, new DisabledInfinispanTelemetry());
      }

      @Override
      void writeException(HotRodHeader header, Throwable cause) {
         failure = cause;
         status = OperationStatus.ServerError;
      }

      @Override
      void writeSuccess(HotRodHeader header) {
         status = OperationStatus.Success;
      }

      @Override
      void writeNotExecuted(HotRodHeader header) {
         status = OperationStatus.OperationNotExecuted;
      }
   }
}
