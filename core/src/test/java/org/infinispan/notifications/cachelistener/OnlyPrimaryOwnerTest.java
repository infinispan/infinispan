package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;

@Test(testName = "notifications.cachelistener.OnlyPrimaryOwnerTest", groups = "unit")
public class OnlyPrimaryOwnerTest {
   CacheNotifierImpl n;
   Cache mockCache;
   PrimaryOwnerCacheListener cl;
   InvocationContext ctx;
   MockCDL cdl = new MockCDL();

   @BeforeMethod
   public void setUp() {
      n = new CacheNotifierImpl();
      mockCache = mock(Cache.class);
      n.injectDependencies(mockCache, cdl, null);
      cl = new PrimaryOwnerCacheListener();
      n.start();
      n.addListener(cl);
      ctx = new NonTxInvocationContext(AnyEquivalence.getInstance());
   }

   private static class MockCDL implements ClusteringDependentLogic {
      boolean isOwner, isPrimaryOwner;
      @Override
      public boolean localNodeIsOwner(Object key) {
         return isOwner;
      }

      @Override
      public boolean localNodeIsPrimaryOwner(Object key) {
         return isPrimaryOwner;
      }

      @Override
      public Address getPrimaryOwner(Object key) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void commitEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx) {
         throw new UnsupportedOperationException();
      }

      @Override
      public List<Address> getOwners(Collection<Object> keys) {
         throw new UnsupportedOperationException();
      }

      @Override
      public List<Address> getOwners(Object key) {
         throw new UnsupportedOperationException();
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Address getAddress() {
         throw new UnsupportedOperationException();
      }

      @Override
      public boolean lock(Object key, boolean noWaitTime) {
         throw new UnsupportedOperationException();
      }

      @Override
      public void unlock(Object key) {
         throw new UnsupportedOperationException();
      }
   }

   public void testOwnership() {
      // Is not owner nor primary owner
      cdl.isOwner = false;
      cdl.isPrimaryOwner = false;
      n.notifyCacheEntryCreated("reject", null, true, ctx, null);
      n.notifyCacheEntryCreated("reject", "v1", false, ctx, null);

      assert !cl.isReceivedPost();
      assert !cl.isReceivedPre();
      assert cl.getInvocationCount() == 0;

      // Is an owner but not primary owner
      cdl.isOwner = true;
      cdl.isPrimaryOwner = false;
      n.notifyCacheEntryCreated("reject", null, true, ctx, null);
      n.notifyCacheEntryCreated("reject", "v1", false, ctx, null);

      assert !cl.isReceivedPost();
      assert !cl.isReceivedPre();
      assert cl.getInvocationCount() == 0;

      // Is primary owner
      cdl.isOwner = true;
      cdl.isPrimaryOwner = true;
      n.notifyCacheEntryCreated("accept", null, true, ctx, null);
      n.notifyCacheEntryCreated("accept", "v1", false, ctx, null);

      assert cl.isReceivedPost();
      assert cl.isReceivedPre();
      assert cl.getInvocationCount() == 2;
      assert cl.getEvents().get(0).getCache() == mockCache;
      assert cl.getEvents().get(0).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getKey().equals("accept");
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(0)).getValue() == null;
      assert cl.getEvents().get(1).getCache() == mockCache;
      assert cl.getEvents().get(1).getType() == Event.Type.CACHE_ENTRY_CREATED;
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getKey().equals("accept");
      assert ((CacheEntryCreatedEvent) cl.getEvents().get(1)).getValue().equals("v1");

   }

}
