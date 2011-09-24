/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.notifications.cachelistener;

import org.easymock.EasyMock;
import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.notifications.cachelistener.event.*;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;

import static org.easymock.classextension.EasyMock.createNiceMock;

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
      n.injectDependencies(mockCache);
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
      n.notifyCacheEntryEvicted("k", "v", null);

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
      InternalCacheEntry ice = InternalEntryFactory.create("k", "v");
      n.notifyCacheEntriesEvicted(Collections.singleton(ice), null);

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
      n.notifyCacheEntryPassivated("k", "v", true, null);
      n.notifyCacheEntryPassivated("k", "v", false, null);

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
