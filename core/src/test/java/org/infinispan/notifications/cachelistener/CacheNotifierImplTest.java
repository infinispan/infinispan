package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplTest")
public class CacheNotifierImplTest extends AbstractInfinispanTest {
   CacheNotifierImpl n;
   Cache mockCache;
   CacheListener cl;
   InvocationContext ctx;

   @BeforeMethod
   public void setUp() {
      n = new CacheNotifierImpl();
      mockCache = mock(Cache.class, RETURNS_DEEP_STUBS);
      Configuration config = mock(Configuration.class, RETURNS_DEEP_STUBS);
      when(mockCache.getAdvancedCache().getStatus()).thenReturn(ComponentStatus.INITIALIZING);
      Answer answer = new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            return Mockito.mock((Class) invocationOnMock.getArguments()[0]);
         }
      };
      when(mockCache.getAdvancedCache().getComponentRegistry().getComponent(any(Class.class))).then(answer);
      when(mockCache.getAdvancedCache().getComponentRegistry().getComponent(any(Class.class), anyString())).then(answer);
      n.injectDependencies(mockCache, new ClusteringDependentLogic.LocalLogic(), null, config,
                           mock(DistributionManager.class), mock(EntryRetriever.class), mock(InternalEntryFactory.class));
      cl = new CacheListener();
      n.start();
      n.addListener(cl);
      ctx = new NonTxInvocationContext(AnyEquivalence.getInstance());
   }

   public void testNotifyCacheEntryCreated() {
      n.notifyCacheEntryCreated("k", "v1", true, ctx, null);
      n.notifyCacheEntryCreated("k", "v1", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getValue() == null;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getValue().equals("v1");
   }

   public void testNotifyCacheEntryModified() {
      n.notifyCacheEntryModified("k", "v2", "v1", null, true, ctx, null);
      n.notifyCacheEntryModified("k", "v2", "v1", null, false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_MODIFIED;
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(0)).getValue().equals("v1");
      assert !((CacheEntryModifiedEvent) cl.getEvents().get(0)).isCreated();
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_MODIFIED;
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(1)).getValue().equals("v2");
      assert !((CacheEntryModifiedEvent) cl.getEvents().get(1)).isCreated();
   }

   public void testNotifyCacheEntryRemoved() {
      n.notifyCacheEntryRemoved("k", "v", null, true, ctx, null);
      n.notifyCacheEntryRemoved("k", "v", null, false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_REMOVED;
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getOldValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_REMOVED;
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getValue() == null;
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getOldValue().equals("v");
   }

   public void testNotifyCacheEntryVisited() {
      n.notifyCacheEntryVisited("k", "v", true, ctx, null);
      n.notifyCacheEntryVisited("k", "v", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_VISITED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryVisitedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_VISITED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryVisitedEvent) cl.getEvents().get(1)).getValue().equals("v");
   }

   public void testNotifyCacheEntryEvicted() {
      n.notifyCacheEntryEvicted("k", "v", null, null);

      assert cl.isReceivedPost();
      assert cl.getInvocationCount() == 1;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_EVICTED;
      Map<Object, Object> entries = ((CacheEntriesEvictedEvent) cl.getEvents().get(0)).getEntries();
      Map.Entry<Object, Object> entry = entries.entrySet().iterator().next();
      assert entry.getKey().equals("k");
      assert entry.getValue().equals("v");
   }

   public void testNotifyCacheEntriesEvicted() {
      InternalCacheEntry ice = TestInternalCacheEntryFactory.create("k", "v");
      n.notifyCacheEntriesEvicted(Collections.singleton(ice), null, null);

      assert cl.isReceivedPost();
      assert cl.getInvocationCount() == 1;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_EVICTED;
      Map<Object, Object> entries = ((CacheEntriesEvictedEvent) cl.getEvents().get(0)).getEntries();
      Map.Entry<Object, Object> entry = entries.entrySet().iterator().next();
      assert entry.getKey().equals("k");
      assert entry.getValue().equals("v");
   }

   public void testNotifyCacheEntryInvalidated() {
      n.notifyCacheEntryInvalidated("k", "v", true, ctx, null);
      n.notifyCacheEntryInvalidated("k", "v", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_INVALIDATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryInvalidatedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_INVALIDATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryInvalidatedEvent) cl.getEvents().get(1)).getValue().equals("v");
   }

   public void testNotifyCacheEntryLoaded() {
      n.notifyCacheEntryLoaded("k", "v", true, ctx, null);
      n.notifyCacheEntryLoaded("k", "v", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_LOADED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryLoadedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_LOADED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryLoadedEvent) cl.getEvents().get(1)).getValue().equals("v");
   }

   public void testNotifyCacheEntryActivated() {
      n.notifyCacheEntryActivated("k", "v", true, ctx, null);
      n.notifyCacheEntryActivated("k", "v", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_ACTIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryActivatedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_ACTIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryActivatedEvent) cl.getEvents().get(1)).getValue().equals("v");
   }

   public void testNotifyCacheEntryPassivated() {
      n.notifyCacheEntryPassivated("k", "v", true, null, null);
      n.notifyCacheEntryPassivated("k", "v", false, null, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_PASSIVATED;
      CacheEntryPassivatedEvent event = (CacheEntryPassivatedEvent) cl.getEvents().get(0);
      assert event.getKey().equals("k");
      assert event.getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_PASSIVATED;
      event = (CacheEntryPassivatedEvent) cl.getEvents().get(1);
      assert event.getKey().equals("k");
      assert event.getValue().equals("v");
   }

   public void testNotifyTransactionCompleted() {
      GlobalTransaction tx = mock(GlobalTransaction.class);
      n.notifyTransactionCompleted(tx, true, ctx);
      n.notifyTransactionCompleted(tx, false, ctx);

      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.TRANSACTION_COMPLETED;
      assert ((TransactionCompletedEvent) cl.getEvents().get(0)).isTransactionSuccessful();
      assert ((TransactionCompletedEvent) cl.getEvents().get(0)).getGlobalTransaction() == tx;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.TRANSACTION_COMPLETED;
      assert !((TransactionCompletedEvent) cl.getEvents().get(1)).isTransactionSuccessful();
      assert ((TransactionCompletedEvent) cl.getEvents().get(1)).getGlobalTransaction() == tx;
   }

   public void testNotifyTransactionRegistered() {
      GlobalTransaction tx = mock(GlobalTransaction.class);
      n.notifyTransactionRegistered(tx, false);
      n.notifyTransactionRegistered(tx, false);

      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.TRANSACTION_REGISTERED;
      assert ((TransactionRegisteredEvent) cl.getEvents().get(0)).getGlobalTransaction() == tx;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.TRANSACTION_REGISTERED;
      assert ((TransactionRegisteredEvent) cl.getEvents().get(1)).getGlobalTransaction() == tx;
   }
}
