package org.infinispan.notifications.cachelistener;

import static org.easymock.classextension.EasyMock.*;

import org.easymock.EasyMock;
import org.infinispan.Cache;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextContainerImpl;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplTest")
public class CacheNotifierImplTest extends AbstractInfinispanTest {
   CacheNotifierImpl n;
   Cache mockCache;
   CacheListener cl;
   InvocationContext ctx;

   @BeforeMethod
   public void setUp() {
      n = new CacheNotifierImpl();
      mockCache = createNiceMock(Cache.class);
      EasyMock.replay(mockCache);
      InvocationContextContainer icc = new InvocationContextContainerImpl();
      n.injectDependencies(icc, mockCache);
      cl = new CacheListener();
      n.start();
      n.addListener(cl);
      ctx = new NonTxInvocationContext();
   }

   public void testNotifyCacheEntryCreated() {
      n.notifyCacheEntryCreated("k", true, ctx);
      n.notifyCacheEntryCreated("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
   }

   public void testNotifyCacheEntryModified() {
      n.notifyCacheEntryModified("k", "v1", true, ctx);
      n.notifyCacheEntryModified("k", "v2", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_MODIFIED;
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(0)).getValue().equals("v1");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_MODIFIED;
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryModifiedEvent) cl.getEvents().get(1)).getValue().equals("v2");
   }

   public void testNotifyCacheEntryRemoved() {
      n.notifyCacheEntryRemoved("k", "v", true, ctx);
      n.notifyCacheEntryRemoved("k", null, false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_REMOVED;
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_REMOVED;
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryRemovedEvent) cl.getEvents().get(1)).getValue() == null;
   }

   public void testNotifyCacheEntryVisited() {
      n.notifyCacheEntryVisited("k", "v", true, ctx);
      n.notifyCacheEntryVisited("k", "v", false, ctx);

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
      n.notifyCacheEntryEvicted("k", "v", true, ctx);
      n.notifyCacheEntryEvicted("k", "v", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_EVICTED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryEvictedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_EVICTED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryEvictedEvent) cl.getEvents().get(1)).getValue().equals("v");
   }

   public void testNotifyCacheEntryInvalidated() {
      n.notifyCacheEntryInvalidated("k", "v", true, ctx);
      n.notifyCacheEntryInvalidated("k", "v", false, ctx);

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
      n.notifyCacheEntryLoaded("k", "v", true, ctx);
      n.notifyCacheEntryLoaded("k", "v", false, ctx);

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
      n.notifyCacheEntryActivated("k", "v", true, ctx);
      n.notifyCacheEntryActivated("k", "v", false, ctx);

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
      n.notifyCacheEntryPassivated("k", "v", true, ctx);
      n.notifyCacheEntryPassivated("k", "v", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_PASSIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert ((CacheEntryPassivatedEvent) cl.getEvents().get(0)).getValue().equals("v");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_PASSIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
      assert ((CacheEntryPassivatedEvent) cl.getEvents().get(1)).getValue().equals("v");
   }

   public void testNotifyTransactionCompleted() {
      GlobalTransaction tx = createNiceMock(GlobalTransaction.class);
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
      InvocationContext ctx = new NonTxInvocationContext();
      GlobalTransaction tx = createNiceMock(GlobalTransaction.class);
      n.notifyTransactionRegistered(tx, ctx);
      n.notifyTransactionRegistered(tx, ctx);

      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.TRANSACTION_REGISTERED;
      assert ((TransactionRegisteredEvent) cl.getEvents().get(0)).getGlobalTransaction() == tx;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.TRANSACTION_REGISTERED;
      assert ((TransactionRegisteredEvent) cl.getEvents().get(1)).getGlobalTransaction() == tx;
   }
}
