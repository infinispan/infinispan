package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.withClientListener;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestClientListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.testing.ExceptionRunnable;
import org.testng.annotations.Test;

import io.netty.channel.Channel;
import io.netty.channel.WriteBufferWaterMark;

@Test(groups = "functional", testName = "server.hotrod.event.HotRodBackpressureWaterMarkTest")
public class HotRodBackpressureWaterMarkTest extends HotRodSingleNodeTest {

   private static final int HIGH_WATER_MARK = 3;
   private static final int LOW_WATER_MARK = 1;
   private static final int MAX_SIZE = 10;

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.listenerBackpressureHighWatermark(HIGH_WATER_MARK)
            .listenerBackpressureLowWatermark(LOW_WATER_MARK)
            .listenerMaxQueueSize(MAX_SIZE);
      return startHotRodServer(cacheManager, builder);
   }

   public void testNormalOperationNoPressure() {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         Object sender = getSender(eventListener.getId());
         AtomicInteger eventSizeRef = TestingUtil.extractField(sender, "eventSize");

         client().put("key-0".getBytes(), 0, 0, "val-0".getBytes());

         // Event should be written immediately — queue drains to 0
         eventually(() -> eventSizeRef.get() == 0, 10, TimeUnit.SECONDS);
      });
   }

   public void testWriteBlockedWhenHighWaterMarkExceeded() throws Exception {
      HotRodClient separateClient = connectClient();
      try {
         TestClientListener listener = new TestClientListener() {
            @Override
            public byte[] getId() {
               return new byte[]{4, 5, 6};
            }
         };
         assertStatus(separateClient.addClientListener(listener, false, Optional.empty(), Optional.empty(), true), Success);

         Object sender = getSender(listener.getId());
         Channel ch = TestingUtil.extractField(sender, "ch");
         AtomicInteger eventSizeRef = TestingUtil.extractField(sender, "eventSize");

         // Make the channel non-writable so events won't be written and futures stay incomplete
         ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(1, 2));
         ch.eventLoop().submit(() -> ch.write(ch.alloc().buffer(64).writeZero(64))).get(5, TimeUnit.SECONDS);
         assertFalse(ch.isWritable(), "Channel should be non-writable");

         // Pre-set event size so the next event triggers backpressure
         eventSizeRef.set(HIGH_WATER_MARK - 1);

         // Fork the put — should be blocked by backpressure since the event can't be written
         Future<Void> putFuture = fork((ExceptionRunnable) () ->
               advancedCache.put("blocked-key".getBytes(), "value".getBytes()));

         // Wait for the event to be queued
         eventually(() -> eventSizeRef.get() >= HIGH_WATER_MARK, 10, TimeUnit.SECONDS);

         // Verify the put is actually blocked
         assertFalse(putFuture.isDone(), "Put should be blocked by backpressure");

         // Unblock all commands to release the per-event future
         Method unblock = sender.getClass().getSuperclass().getDeclaredMethod("unblockCommands");
         unblock.setAccessible(true);
         unblock.invoke(sender);

         // The put should now complete
         eventually(putFuture::isDone, 10, TimeUnit.SECONDS);
      } finally {
         killClient(separateClient);
      }
   }

   public void testEventWriteCompletesBackpressureFuture() {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         Object sender = getSender(eventListener.getId());
         AtomicInteger eventSizeRef = TestingUtil.extractField(sender, "eventSize");

         // Perform enough puts to exceed the high water mark
         for (int i = 0; i < HIGH_WATER_MARK + 2; i++) {
            client().put(("key-" + i).getBytes(), 0, 0, ("val-" + i).getBytes());
         }

         // All events should be written and their per-event futures completed — queue drains to 0
         eventually(() -> eventSizeRef.get() == 0, 10, TimeUnit.SECONDS);
      });
   }

   public void testHardCapClosesChannel() throws Exception {
      HotRodClient separateClient = connectClient();
      try {
         TestClientListener listener = new TestClientListener() {
            @Override
            public byte[] getId() {
               return new byte[]{7, 8, 9};
            }
         };
         assertStatus(separateClient.addClientListener(listener, false, Optional.empty(), Optional.empty(), true), Success);

         Object sender = getSender(listener.getId());
         Channel ch = TestingUtil.extractField(sender, "ch");
         AtomicInteger eventSizeRef = TestingUtil.extractField(sender, "eventSize");

         assertTrue(ch.isOpen(), "Channel should be open");
         eventSizeRef.set(MAX_SIZE - 1);

         advancedCache.put("trigger-hard-cap".getBytes(), "value".getBytes());

         eventually(() -> !ch.isOpen(), 10, TimeUnit.SECONDS);
      } finally {
         killClient(separateClient);
      }
   }

   private Object getSender(byte[] listenerId) {
      Object registry = hotRodServer.getClientListenerRegistry();
      ConcurrentMap<WrappedByteArray, Object> senders = TestingUtil.extractField(registry, "eventSenders");
      Object sender = senders.get(new WrappedByteArray(listenerId));
      assertNotNull(sender, "Event sender should exist for listener");
      return sender;
   }
}
