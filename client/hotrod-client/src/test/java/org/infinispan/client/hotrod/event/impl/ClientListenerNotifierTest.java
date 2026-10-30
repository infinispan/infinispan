package org.infinispan.client.hotrod.event.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
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
      ReconnectTask task = new ReconnectTask(dispatcher, addr1);

      // When dispatcher is stopped (not running), running the task should immediately stop/cancel
      task.run();
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

   private static class TestDispatcher extends EventDispatcher<Object> {
      final AtomicInteger failoverInvocations = new AtomicInteger();
      TestDispatcher(byte[] listenerId, Object listener) {
         super("testCache", listener, listenerId, null);
      }

      @Override
      public CompletableFuture<Void> executeFailover(SocketAddress failedAddress) {
         failoverInvocations.incrementAndGet();
         return CompletableFuture.completedFuture(null);
      }

      @Override
      protected void invokeEvent(Object event) {}
   }
}
