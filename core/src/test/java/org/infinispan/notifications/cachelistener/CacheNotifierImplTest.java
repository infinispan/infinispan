package org.infinispan.notifications.cachelistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.Map;

import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.MockBasicComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.Event.Type;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplTest")
public class CacheNotifierImplTest extends AbstractInfinispanTest {
   CacheNotifierImpl n;
   EncoderCache mockCache;
   CacheListener cl;
   InvocationContext ctx;

   @BeforeMethod
   public void setUp() {
      n = new CacheNotifierImpl();
      mockCache = mock(EncoderCache.class);
      EmbeddedCacheManager cacheManager = mock(EmbeddedCacheManager.class);
      when(mockCache.getCacheManager()).thenReturn(cacheManager);
      when(mockCache.getAdvancedCache()).thenReturn(mockCache);
      when(mockCache.getKeyDataConversion()).thenReturn(DataConversion.IDENTITY_KEY);
      when(mockCache.getValueDataConversion()).thenReturn(DataConversion.IDENTITY_VALUE);
      when(mockCache.getStatus()).thenReturn(ComponentStatus.INITIALIZING);
      ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
      when(mockCache.getComponentRegistry()).thenReturn(componentRegistry);
      MockBasicComponentRegistry mockRegistry = new MockBasicComponentRegistry();
      when(componentRegistry.getComponent(BasicComponentRegistry.class)).thenReturn(mockRegistry);
      mockRegistry.registerMocks(RpcManager.class, CommandsFactory.class);
      mockRegistry.registerMock(KnownComponentNames.INTERNAL_MARSHALLER, Marshaller.class);
      ClusteringDependentLogic.LocalLogic cdl = new ClusteringDependentLogic.LocalLogic();
      Configuration config = new ConfigurationBuilder().build();
      cdl.init(null, config, mock(KeyPartitioner.class));
      ClusterEventManager cem = mock(ClusterEventManager.class);
      when(cem.sendEvents(any())).thenReturn(CompletableFutures.completedNull());
      BlockingManager handler = mock(BlockingManager.class);
      when(handler.continueOnNonBlockingThread(any(), any())).thenReturn(CompletableFutures.completedNull());
      TestingUtil.inject(n, mockCache, cdl, config, mockRegistry,
                         mock(InternalEntryFactory.class), cem, mock(KeyPartitioner.class), new FakeEncoderRegistry(),
                         TestingUtil.named(KnownComponentNames.ASYNC_NOTIFICATION_EXECUTOR, new WithinThreadExecutor()),
                         handler);
      cl = new CacheListener();
      n.start();
      addListener();
      ctx = new NonTxInvocationContext(null);
   }

   protected void addListener() {
      n.addListener(cl);
   }

   protected Object getExpectedEventValue(Object key, Object val, Type t) {
      return val;
   }

   public void testNotifyCacheEntryCreated() {
      n.notifyCacheEntryCreated("k", "v1", null, true, ctx, null);
      n.notifyCacheEntryCreated("k", "v1", null, false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_CREATED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", null, Event.Type.CACHE_ENTRY_CREATED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_CREATED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", "v1", Event.Type.CACHE_ENTRY_CREATED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
   }

   public void testNotifyCacheEntryModified() {
      n.notifyCacheEntryModified("k", "v2", null, "v1", null, true, ctx, null);
      n.notifyCacheEntryModified("k", "v2", null, "v1", null, false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryModifiedEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", "v1", Event.Type.CACHE_ENTRY_MODIFIED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertTrue(!((CacheEntryModifiedEvent) cl.getEvents().get(0)).isCreated());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryModifiedEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", "v2", Event.Type.CACHE_ENTRY_MODIFIED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
      assertTrue(!((CacheEntryModifiedEvent) cl.getEvents().get(1)).isCreated());
   }

   public void testNotifyCacheEntryRemoved() {
      n.notifyCacheEntryRemoved("k", "v", null, true, ctx, null);
      n.notifyCacheEntryRemoved("k", "v", null, false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_REMOVED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_REMOVED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals("v", ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getOldValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_REMOVED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", null, Event.Type.CACHE_ENTRY_REMOVED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
      assertEquals("v", ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getOldValue());
   }

   public void testNotifyCacheEntryVisited() {
      n.notifyCacheEntryVisited("k", "v", true, ctx, null);
      n.notifyCacheEntryVisited("k", "v", false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_VISITED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_VISITED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_VISITED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_VISITED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
   }

   public void testNotifyCacheEntryEvicted() {
      n.notifyCacheEntriesEvicted(Collections.singleton(new ImmortalCacheEntry("k", "v")), null, null);

      assertTrue(cl.isReceivedPost());
      assertEquals(1, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_EVICTED, cl.getEvents().get(0).getType());
      Map<Object, Object> entries = ((CacheEntriesEvictedEvent) cl.getEvents().get(0)).getEntries();
      Map.Entry<Object, Object> entry = entries.entrySet().iterator().next();
      assertEquals("k", entry.getKey());
      assertEquals("v", entry.getValue());
   }

   public void testNotifyCacheEntriesEvicted() {
      InternalCacheEntry ice = TestInternalCacheEntryFactory.create("k", "v");
      n.notifyCacheEntriesEvicted(Collections.singleton(ice), null, null);

      assertTrue(cl.isReceivedPost());
      assertEquals(1, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_EVICTED, cl.getEvents().get(0).getType());
      Map<Object, Object> entries = ((CacheEntriesEvictedEvent) cl.getEvents().get(0)).getEntries();
      Map.Entry<Object, Object> entry = entries.entrySet().iterator().next();
      assertEquals("k", entry.getKey());
      assertEquals("v", entry.getValue());
   }

   public void testNotifyCacheEntryExpired() {
      n.notifyCacheEntryExpired("k", "v", null, null);

      assertTrue(cl.isReceivedPost());
      assertEquals(cl.getInvocationCount(), 1);
      assertEquals(cl.getEvents().get(0).getCache(), mockCache);
      assertEquals(cl.getEvents().get(0).getType(), Event.Type.CACHE_ENTRY_EXPIRED);
      CacheEntryExpiredEvent expiredEvent = ((CacheEntryExpiredEvent) cl.getEvents().get(0));
      assertEquals(expiredEvent.getKey(), "k");
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_EXPIRED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
   }

   public void testNotifyCacheEntryInvalidated() {
      n.notifyCacheEntryInvalidated("k", "v", null, true, ctx, null);
      n.notifyCacheEntryInvalidated("k", "v", null, false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_INVALIDATED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_INVALIDATED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_INVALIDATED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_INVALIDATED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
   }

   public void testNotifyCacheEntryLoaded() {
      n.notifyCacheEntryLoaded("k", "v", true, ctx, null);
      n.notifyCacheEntryLoaded("k", "v", false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_LOADED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_LOADED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_LOADED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_LOADED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
   }

   public void testNotifyCacheEntryActivated() {
      n.notifyCacheEntryActivated("k", "v", true, ctx, null);
      n.notifyCacheEntryActivated("k", "v", false, ctx, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_ACTIVATED, cl.getEvents().get(0).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(0)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_ACTIVATED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_ACTIVATED, cl.getEvents().get(1).getType());
      assertEquals("k", ((CacheEntryEvent) cl.getEvents().get(1)).getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_ACTIVATED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
   }

   public void testNotifyCacheEntryPassivated() {
      n.notifyCacheEntryPassivated("k", "v", true, null, null);
      n.notifyCacheEntryPassivated("k", "v", false, null, null);

      assertTrue(cl.isReceivedPost());
      assertTrue(cl.isReceivedPre());
      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_PASSIVATED, cl.getEvents().get(0).getType());
      CacheEntryPassivatedEvent event = (CacheEntryPassivatedEvent) cl.getEvents().get(0);
      assertEquals("k", event.getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_PASSIVATED),
            ((CacheEntryEvent) cl.getEvents().get(0)).getValue());
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.CACHE_ENTRY_PASSIVATED, cl.getEvents().get(1).getType());
      event = (CacheEntryPassivatedEvent) cl.getEvents().get(1);
      assertEquals("k", event.getKey());
      assertEquals(getExpectedEventValue("k", "v", Event.Type.CACHE_ENTRY_PASSIVATED),
            ((CacheEntryEvent) cl.getEvents().get(1)).getValue());
   }

   public void testNotifyTransactionCompleted() {
      GlobalTransaction tx = mock(GlobalTransaction.class);
      n.notifyTransactionCompleted(tx, true, ctx);
      n.notifyTransactionCompleted(tx, false, ctx);

      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.TRANSACTION_COMPLETED, cl.getEvents().get(0).getType());
      assertTrue(((TransactionCompletedEvent) cl.getEvents().get(0)).isTransactionSuccessful());
      assertEquals(((TransactionCompletedEvent) cl.getEvents().get(0)).getGlobalTransaction(), tx);
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.TRANSACTION_COMPLETED, cl.getEvents().get(1).getType());
      assertTrue(!((TransactionCompletedEvent) cl.getEvents().get(1)).isTransactionSuccessful());
      assertEquals(((TransactionCompletedEvent) cl.getEvents().get(1)).getGlobalTransaction(), tx);
   }

   public void testNotifyTransactionRegistered() {
      GlobalTransaction tx = mock(GlobalTransaction.class);
      n.notifyTransactionRegistered(tx, false);
      n.notifyTransactionRegistered(tx, false);

      assertEquals(2, cl.getInvocationCount());
      assertEquals(mockCache, cl.getEvents().get(0).getCache());
      assertEquals(Event.Type.TRANSACTION_REGISTERED, cl.getEvents().get(0).getType());
      assertEquals(((TransactionRegisteredEvent) cl.getEvents().get(0)).getGlobalTransaction(), tx);
      assertEquals(mockCache, cl.getEvents().get(1).getCache());
      assertEquals(Event.Type.TRANSACTION_REGISTERED, cl.getEvents().get(1).getType());
      assertEquals(((TransactionRegisteredEvent) cl.getEvents().get(1)).getGlobalTransaction(), tx);
   }
}
