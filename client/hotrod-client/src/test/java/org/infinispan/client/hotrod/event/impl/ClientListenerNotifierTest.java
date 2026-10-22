package org.infinispan.client.hotrod.event.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.AddPrimaryNearCacheClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.DefaultCacheOperationsFactory;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "client.hotrod.event.impl.ClientListenerNotifierTest")
public class ClientListenerNotifierTest extends AbstractInfinispanTest {

   private ClientListenerNotifier notifier;

   @BeforeMethod
   public void setup() {
      notifier = new ClientListenerNotifier(null, new ConfigurationBuilder().build());
   }

   @AfterMethod
   public void tearDown() {
      if (notifier != null) {
         notifier.stop();
      }
   }

   public void testLazyDispatcherCreation() {
      byte[] listenerId = new byte[]{1, 2, 3};
      Object listener = new Object();
      SocketAddress addr1 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress addr2 = InetSocketAddress.createUnresolved("127.0.0.1", 11223);
      SocketAddress addr3 = InetSocketAddress.createUnresolved("127.0.0.1", 11224);

      AtomicInteger factoryInvocations = new AtomicInteger();

      EventDispatcher<?> d1 = notifier.addDispatcher(listenerId, addr1, listener, (id, l) -> {
         factoryInvocations.incrementAndGet();
         return new TestDispatcher(id, l);
      });

      assertNotNull(d1);
      assertEquals(1, factoryInvocations.get());
      assertEquals(Set.of(addr1), notifier.findAddresses(listenerId));

      // Adding a second address for the same listener ID should NOT invoke the factory
      EventDispatcher<?> d2 = notifier.addDispatcher(listenerId, addr2, listener, (id, l) -> {
         factoryInvocations.incrementAndGet();
         return new TestDispatcher(id, l);
      });

      assertSame(d1, d2);
      assertEquals(1, factoryInvocations.get());
      assertEquals(Set.of(addr1, addr2), notifier.findAddresses(listenerId));

      // Adding a third address should also NOT invoke the factory
      EventDispatcher<?> d3 = notifier.addDispatcher(listenerId, addr3, listener, (id, l) -> {
         factoryInvocations.incrementAndGet();
         return new TestDispatcher(id, l);
      });

      assertSame(d1, d3);
      assertEquals(1, factoryInvocations.get());
      assertEquals(Set.of(addr1, addr2, addr3), notifier.findAddresses(listenerId));
   }

   public void testAddressRemovalLifecycle() {
      byte[] listenerId = new byte[]{4, 5, 6};
      Object listener = new Object();
      SocketAddress addr1 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress addr2 = InetSocketAddress.createUnresolved("127.0.0.1", 11223);

      AtomicInteger factoryInvocations = new AtomicInteger();

      EventDispatcher<?> d1 = notifier.addDispatcher(listenerId, addr1, listener, (id, l) -> {
         factoryInvocations.incrementAndGet();
         return new TestDispatcher(id, l);
      });
      notifier.addDispatcher(listenerId, addr2, listener, (id, l) -> {
         factoryInvocations.incrementAndGet();
         return new TestDispatcher(id, l);
      });

      notifier.startClientListener(listenerId);
      assertTrue(notifier.isListenerConnected(listenerId));

      // Removing addr1 should keep the dispatcher alive because addr2 is still registered
      EventDispatcher<?> removed1 = notifier.removeClientListener(listenerId, addr1);
      assertSame(d1, removed1);
      assertTrue(d1.isRunning());
      assertTrue(notifier.isListenerConnected(listenerId));
      assertEquals(Set.of(addr2), notifier.findAddresses(listenerId));

      // Removing addr2 should stop the dispatcher and remove it from the notifier
      EventDispatcher<?> removed2 = notifier.removeClientListener(listenerId, addr2);
      assertSame(d1, removed2);
      assertFalse(d1.isRunning());
      assertFalse(notifier.isListenerConnected(listenerId));
      assertTrue(notifier.findAddresses(listenerId).isEmpty());

      // Registering again should lazily create a new dispatcher
      EventDispatcher<?> dFresh = notifier.addDispatcher(listenerId, addr1, listener, (id, l) -> {
         factoryInvocations.incrementAndGet();
         return new TestDispatcher(id, l);
      });

      assertEquals(2, factoryInvocations.get());
      assertTrue(dFresh != d1);
      assertEquals(Set.of(addr1), notifier.findAddresses(listenerId));
   }

   public void testEagerAddDispatcherDelegation() {
      byte[] listenerId = new byte[]{7, 8, 9};
      Object listener = new Object();
      SocketAddress addr1 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress addr2 = InetSocketAddress.createUnresolved("127.0.0.1", 11223);

      TestDispatcher dispatcher = new TestDispatcher(listenerId, listener);
      dispatcher.addAddress(addr1);

      notifier.addDispatcher(dispatcher);
      assertEquals(Set.of(addr1), notifier.findAddresses(listenerId));

      // Eagerly adding a second dispatcher for the same ID merges addresses
      TestDispatcher dispatcher2 = new TestDispatcher(listenerId, listener);
      dispatcher2.addAddress(addr2);

      notifier.addDispatcher(dispatcher2);
      assertEquals(Set.of(addr1, addr2), notifier.findAddresses(listenerId));
   }

   public void testReconnectTaskCancelsWhenDispatcherStopped() {
      byte[] listenerId = new byte[]{10, 11, 12};
      Object listener = new Object();
      SocketAddress addr1 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);

      TestDispatcher dispatcher = new TestDispatcher(listenerId, listener);
      ReconnectTask task = new ReconnectTask(dispatcher);

      // When dispatcher is stopped (not running), running the task should immediately stop/cancel
      task.run();
      assertEquals(0, dispatcher.failoverInvocations.get());
   }

   public void testReconnectTaskContinuesWhenDispatcherStoppedByFailover() {
      byte[] listenerId = new byte[]{20, 21, 22};
      Object listener = new Object();
      SocketAddress addr1 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);

      TestDispatcher dispatcher = new TestDispatcher(listenerId, listener);
      notifier.addDispatcher(listenerId, addr1, listener, (id, l) -> dispatcher);
      notifier.startClientListener(listenerId);

      // Failing over the only address stops the dispatcher; the reconnect must still be attempted
      notifier.failoverClientListener(listenerId, addr1);
      assertFalse(dispatcher.isRunning());
      assertEquals(1, dispatcher.failoverInvocations.get());

      ReconnectTask task = new ReconnectTask(dispatcher);
      task.run();
      assertEquals(2, dispatcher.failoverInvocations.get());
   }

   public void testRemovalFencesReconnectBeforeCleanup() {
      byte[] listenerId = new byte[]{23, 24, 25};
      SocketAddress address = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      AtomicInteger cleanups = new AtomicInteger();
      TestDispatcher dispatcher = new TestDispatcher(listenerId, new Object(), cleanups::incrementAndGet);
      notifier.addDispatcher(listenerId, address, dispatcher.listener, (id, listener) -> dispatcher);
      notifier.startClientListener(listenerId);

      notifier.markRemoved(listenerId);
      notifier.markRemoved(listenerId);

      assertTrue(dispatcher.isTerminated());
      assertFalse(dispatcher.start());
      // Server unregistration still needs the addresses, and cleanup must wait for it.
      assertEquals(Set.of(address), notifier.findAddresses(listenerId));
      assertEquals(0, cleanups.get());
      new ReconnectTask(dispatcher).run();
      assertEquals(0, dispatcher.failoverInvocations.get());

      notifier.removeClientListener(listenerId, address);
      assertFalse(dispatcher.isRunning());
      assertTrue(dispatcher.isTerminated());
      assertEquals(Set.of(), dispatcher.addresses());
      assertEquals(1, cleanups.get());
      assertFalse(dispatcher.stop());
      assertFalse(dispatcher.stopForFailover());
      assertFalse(dispatcher.start());
      assertEquals(1, cleanups.get());
   }

   public void testFailoverStoppedDispatcherCanRestart() {
      AtomicInteger cleanups = new AtomicInteger();
      TestDispatcher dispatcher = new TestDispatcher(new byte[]{26, 27, 28}, new Object(), cleanups::incrementAndGet);
      assertTrue(dispatcher.start());
      assertTrue(dispatcher.stopForFailover());
      assertTrue(dispatcher.isStoppedByFailover());
      assertEquals(1, cleanups.get());
      assertFalse(dispatcher.stopForFailover());
      assertEquals(1, cleanups.get());
      assertTrue(dispatcher.start());
      assertTrue(dispatcher.isRunning());
      assertFalse(dispatcher.isStoppedByFailover());

      assertTrue(dispatcher.stopForFailover());
      dispatcher.stop();
      assertFalse(dispatcher.isStoppedByFailover());
      assertEquals(2, cleanups.get());
      new ReconnectTask(dispatcher).run();
      assertEquals(0, dispatcher.failoverInvocations.get());
   }

   public void testRemovalDuringFailoverCleanupIsTerminal() {
      byte[] listenerId = new byte[]{32, 33, 34};
      SocketAddress address = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      AtomicInteger cleanups = new AtomicInteger();
      TestDispatcher dispatcher = new TestDispatcher(listenerId, new Object(), () -> {
         cleanups.incrementAndGet();
         notifier.removeClientListener(listenerId);
      });
      notifier.addDispatcher(listenerId, address, dispatcher.listener, (id, listener) -> dispatcher);
      notifier.startClientListener(listenerId);

      // Removal wins after the failover stop transition, before failover processing continues.
      notifier.failoverClientListener(listenerId, address);

      assertTrue(dispatcher.isTerminated());
      assertFalse(dispatcher.isStoppedByFailover());
      assertFalse(dispatcher.start());
      new ReconnectTask(dispatcher).run();
      assertEquals(0, dispatcher.failoverInvocations.get());
      assertEquals(1, cleanups.get());
   }

   public void testRemovalBeforeStartIsTerminal() {
      TestDispatcher dispatcher = new TestDispatcher(new byte[]{29, 30, 31}, new Object());
      dispatcher.terminate();
      assertFalse(dispatcher.start());
      assertFalse(dispatcher.stopForFailover());
      assertTrue(dispatcher.isTerminated());
      new ReconnectTask(dispatcher).run();
      assertEquals(0, dispatcher.failoverInvocations.get());
   }

   public void testAddAddressById() {
      byte[] listenerId = new byte[]{13, 14, 15};
      Object listener = new Object();
      SocketAddress addr1 = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress addr2 = InetSocketAddress.createUnresolved("127.0.0.1", 11223);

      notifier.addDispatcher(listenerId, addr1, listener, (id, l) -> new TestDispatcher(id, l));

      // addAddress by listener-id should add the address to the existing dispatcher
      notifier.addAddress(listenerId, addr2);
      assertEquals(Set.of(addr1, addr2), notifier.findAddresses(listenerId));

      // addAddress for an unknown listener id is a no-op (no exception thrown)
      notifier.addAddress(new byte[]{99}, addr1);
   }

   private PrimaryNearCacheEventDispatcher primaryDispatcher(byte[] listenerId, InvalidatedNearRemoteCache<?, ?> inrc) {
      InternalRemoteCache<?, ?> cache = org.mockito.Mockito.mock(InternalRemoteCache.class);
      org.mockito.Mockito.when(cache.getName()).thenReturn("testCache");
      AddPrimaryNearCacheClientListenerOperation op =
            new AddPrimaryNearCacheClientListenerOperation(cache, new Object(), listenerId, 1024);
      org.mockito.Mockito.when(inrc.getName()).thenReturn("testCache");
      return PrimaryNearCacheEventDispatcher.create(op, inrc);
   }

   public void testSingleAddressDispatchersIgnoreStaleRemoval() {
      InternalRemoteCache<?, ?> cache = org.mockito.Mockito.mock(InternalRemoteCache.class);
      ClientListenerOperation op = new DefaultCacheOperationsFactory(cache).newAddClientListenerOperation(new Object());
      SocketAddress first = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress second = InetSocketAddress.createUnresolved("127.0.0.1", 11223);
      EventDispatcher<?>[] dispatchers = {
            ClientEventDispatcher.create(op, first, null, cache),
            new CounterEventDispatcher(new byte[]{1}, new ConcurrentHashMap<>(), first,
                  () -> CompletableFuture.completedFuture(null), null)
      };
      for (EventDispatcher<?> dispatcher : dispatchers) {
         dispatcher.start();
         dispatcher.addAddress(second);
         assertEquals(Set.of(second), dispatcher.addresses());
         assertFalse(dispatcher.removeAddress(first));
         dispatcher.replaceAddress(first, first);
         assertEquals(Set.of(second), dispatcher.addresses());
         dispatcher.replaceAddress(second, first);
         assertEquals(Set.of(first), dispatcher.addresses());
         dispatcher.stop();
         assertEquals(Set.of(), dispatcher.addresses());
      }
   }

   public void testBothCacheDispatchersDeliverEventsAndExposeDataFormat() {
      InvalidatedNearRemoteCache<?, ?> cache = org.mockito.Mockito.mock(InvalidatedNearRemoteCache.class);
      DataFormat format = DataFormat.builder().build();
      org.mockito.Mockito.when(cache.getDataFormat()).thenReturn(format);
      RecordingListener listener = new RecordingListener();
      ClientListenerOperation op = new DefaultCacheOperationsFactory(cache).newAddClientListenerOperation(listener);
      AbstractClientEventDispatcher[] dispatchers = {
            ClientEventDispatcher.create(op, null),
            PrimaryNearCacheEventDispatcher.create(op, cache)
      };
      ClientCacheEntryCreatedEvent<?> event = org.mockito.Mockito.mock(ClientCacheEntryCreatedEvent.class);
      org.mockito.Mockito.when(event.getType()).thenReturn(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      for (AbstractClientEventDispatcher dispatcher : dispatchers) {
         notifier.addDispatcher(dispatcher);
         assertSame(format, notifier.getCacheDataFormat(op.listenerId));
         notifier.invokeEvent(op.listenerId, event);
         notifier.removeClientListener(op.listenerId);
      }
      assertEquals(2, listener.events.get());
   }

   public void testPrimaryListenerLossIsNotFailedOverNorRetried() {
      InvalidatedNearRemoteCache<?, ?> inrc = org.mockito.Mockito.mock(InvalidatedNearRemoteCache.class);
      byte[] listenerId = new byte[]{4, 5, 6};
      SocketAddress failedAddr = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress otherAddr = InetSocketAddress.createUnresolved("127.0.0.1", 11223);
      PrimaryNearCacheEventDispatcher dispatcher = primaryDispatcher(listenerId, inrc);
      notifier.addDispatcher(dispatcher);
      assertTrue(notifier.addAddress(listenerId, failedAddr));
      assertTrue(notifier.addAddress(listenerId, otherAddr));
      notifier.startClientListener(listenerId);
      io.netty.channel.Channel channel = org.mockito.Mockito.mock(io.netty.channel.Channel.class);

      notifier.failoverClientListener(listenerId, failedAddr, channel);

      // The near cache is told to fence the owner, and nothing else happens: no registration, no balancer, no timer
      org.mockito.Mockito.verify(inrc).handlePrimaryListenerDisconnected(failedAddr, channel);
      org.mockito.Mockito.verify(inrc, org.mockito.Mockito.never()).getDispatcher();
      assertEquals(Set.of(otherAddr), notifier.findAddresses(listenerId));
      notifier.failoverClientListener(listenerId, otherAddr, channel);
      org.mockito.Mockito.verify(inrc).handlePrimaryListenerDisconnected(otherAddr, channel);
      // The dispatcher is kept for events from newly added owners
      assertEquals(Set.of(), notifier.findAddresses(listenerId));
      assertNotNull(notifier.findListenerId(dispatcher.listener));
   }

   public void testUserRemovalIsTerminalForAddressesAndFailover() {
      byte[] listenerId = new byte[]{7, 8, 9};
      Object listener = new Object();
      SocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 11222);
      SocketAddress other = InetSocketAddress.createUnresolved("127.0.0.1", 11223);
      TestDispatcher dispatcher = (TestDispatcher) notifier.addDispatcher(listenerId, addr, listener, TestDispatcher::new);
      dispatcher.start();

      // The user removes the listener while a reconnect is queued or a registration is in flight
      notifier.markRemoved(listenerId);
      assertTrue(dispatcher.isTerminated());
      notifier.removeClientListener(listenerId);

      // A late registration can't bring it back
      assertFalse(notifier.addAddress(listenerId, other));
      assertNull((Object) notifier.findListenerId(listener));
      assertEquals(Set.of(), notifier.findAddresses(listenerId));

      ReconnectTask task = new ReconnectTask(dispatcher);
      task.run();
      assertEquals(0, dispatcher.failoverInvocations.get());
   }

   public void testRemoveClientListenerCancelsPendingFailover() {
      byte[] listenerId = new byte[]{1, 2, 3};
      Object listener = new Object();
      SocketAddress addr = InetSocketAddress.createUnresolved("127.0.0.1", 11222);

      TestDispatcher dispatcher = (TestDispatcher) notifier.addDispatcher(listenerId, addr, listener, TestDispatcher::new);
      dispatcher.start();
      assertTrue(dispatcher.isRunning());

      // Trigger failover on the only address: dispatcher loses all addresses, moves to pendingFailover and is marked stoppedByFailover
      notifier.failoverClientListener(listenerId, addr);
      assertTrue(dispatcher.isStoppedByFailover());
      assertFalse(dispatcher.isRunning());

      // findListenerId must still find it in pendingFailover
      byte[] foundId = notifier.findListenerId(listener);
      assertNotNull(foundId);
      assertEquals(listenerId, foundId);

      // User calls removeClientListener
      EventDispatcher<?> removed = notifier.removeClientListener(listenerId);
      assertSame(dispatcher, removed);
      assertFalse(dispatcher.isStoppedByFailover());
      assertFalse(dispatcher.isRunning());
      assertTrue(dispatcher.isTerminated());
      assertFalse(dispatcher.start());
      assertFalse(dispatcher.stopForFailover());
      new ReconnectTask(dispatcher).run();
      assertEquals(1, dispatcher.failoverInvocations.get());

      // Should no longer be found
      assertNull((Object) notifier.findListenerId(listener));
   }

   private static class TestDispatcher extends EventDispatcher<Object> implements FailoverEventDispatcher {
      private final Set<SocketAddress> addresses = ConcurrentHashMap.newKeySet();
      final AtomicInteger failoverInvocations = new AtomicInteger();
      TestDispatcher(byte[] listenerId, Object listener) {
         this(listenerId, listener, null);
      }

      TestDispatcher(byte[] listenerId, Object listener, Runnable cleanup) {
         super("testCache", listener, listenerId, cleanup);
      }

      @Override
      public CompletableFuture<Void> executeFailover() {
         failoverInvocations.incrementAndGet();
         return CompletableFuture.completedFuture(null);
      }

      @Override
      public void addAddress(SocketAddress address) {
         if (address != null) {
            addresses.add(address);
         }
      }

      @Override
      public boolean removeAddress(SocketAddress address) {
         return addresses.remove(address);
      }

      @Override
      public Set<SocketAddress> addresses() {
         return Set.copyOf(addresses);
      }

      @Override
      protected void clearAddresses() {
         addresses.clear();
      }

      @Override
      protected void invokeEvent(Object event) {}
   }

   @ClientListener
   public static class RecordingListener {
      final AtomicInteger events = new AtomicInteger();

      @ClientCacheEntryCreated
      public void created(ClientCacheEntryCreatedEvent<?> event) {
         events.incrementAndGet();
      }
   }
}
