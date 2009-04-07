package org.infinispan.notifications.cachelistener;

import org.easymock.EasyMock;
import static org.easymock.EasyMock.createNiceMock;
import org.infinispan.Cache;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextImpl;
import org.infinispan.factories.context.ContextFactory;
import org.infinispan.invocation.InvocationContextContainer;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

@Test(groups = "unit", testName = "notifications.cachelistener.CacheNotifierImplTest")
public class CacheNotifierImplTest {
   CacheNotifierImpl n;
   Cache mockCache;
   CacheListener cl;
   InvocationContext ctx;

   @BeforeMethod
   public void setUp() {
      n = new CacheNotifierImpl();
      mockCache = createNiceMock(Cache.class);
      EasyMock.replay(mockCache);
      InvocationContextContainer icc = new InvocationContextContainer();
      icc.injectContextFactory(new ContextFactory());
      n.injectDependencies(icc, mockCache);
      cl = new CacheListener();
      n.start();
      n.addListener(cl);
      ctx = new InvocationContextImpl();
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
      n.notifyCacheEntryVisited("k", true, ctx);
      n.notifyCacheEntryVisited("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_VISITED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_VISITED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");

   }

   public void testNotifyCacheEntryEvicted() {
      n.notifyCacheEntryEvicted("k", true, ctx);
      n.notifyCacheEntryEvicted("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_EVICTED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_EVICTED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
   }

   public void testNotifyCacheEntryInvalidated() {
      n.notifyCacheEntryInvalidated("k", true, ctx);
      n.notifyCacheEntryInvalidated("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_INVALIDATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_INVALIDATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
   }

   public void testNotifyCacheEntryLoaded() {
      n.notifyCacheEntryLoaded("k", true, ctx);
      n.notifyCacheEntryLoaded("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_LOADED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_LOADED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
   }

   public void testNotifyCacheEntryActivated() {
      n.notifyCacheEntryActivated("k", true, ctx);
      n.notifyCacheEntryActivated("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_ACTIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_ACTIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
   }

   public void testNotifyCacheEntryPassivated() {
      n.notifyCacheEntryPassivated("k", true, ctx);
      n.notifyCacheEntryPassivated("k", false, ctx);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_PASSIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(0)).getKey().equals("k");
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_PASSIVATED;
      assert ((CacheEntryEvent) cl.getEvents().get(1)).getKey().equals("k");
   }

   public void testNotifyTransactionCompleted() {
      Transaction tx = createNiceMock(Transaction.class);
      n.notifyTransactionCompleted(tx, true, ctx);
      n.notifyTransactionCompleted(tx, false, ctx);

      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.TRANSACTION_COMPLETED;
      assert ((TransactionCompletedEvent) cl.getEvents().get(0)).isTransactionSuccessful();
      assert ((TransactionCompletedEvent) cl.getEvents().get(0)).getTransaction() == tx;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.TRANSACTION_COMPLETED;
      assert !((TransactionCompletedEvent) cl.getEvents().get(1)).isTransactionSuccessful();
      assert ((TransactionCompletedEvent) cl.getEvents().get(1)).getTransaction() == tx;
   }

   public void testNotifyTransactionRegistered() {
      InvocationContext ctx = new InvocationContextImpl();
      Transaction tx = createNiceMock(Transaction.class);
      n.notifyTransactionRegistered(tx, ctx);
      n.notifyTransactionRegistered(tx, ctx);

      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.TRANSACTION_REGISTERED;
      assert ((TransactionRegisteredEvent) cl.getEvents().get(0)).getTransaction() == tx;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.TRANSACTION_REGISTERED;
      assert ((TransactionRegisteredEvent) cl.getEvents().get(1)).getTransaction() == tx;
   }
}
