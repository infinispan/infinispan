package org.infinispan.notifications.cachelistener;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.compat.TypeConverter;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.interceptors.impl.WrappedByteArrayConverter;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
      mockCache = mock(Cache.class, RETURNS_DEEP_STUBS);
      Configuration config = mock(Configuration.class, RETURNS_DEEP_STUBS);
      when(mockCache.getAdvancedCache().getStatus()).thenReturn(ComponentStatus.INITIALIZING);
      when(mockCache.getAdvancedCache().getComponentRegistry().getComponent(any(Class.class))).then(
            invocationOnMock -> Mockito.mock((Class<?>) invocationOnMock.getArguments()[0]));
      when(mockCache.getAdvancedCache().getComponentRegistry().getComponent(TypeConverter.class)).thenReturn(
            new WrappedByteArrayConverter());
      when(mockCache.getAdvancedCache().getComponentRegistry().getComponent(any(Class.class), anyString())).then(
            invocationOnMock -> Mockito.mock((Class<?>) invocationOnMock.getArguments()[0]));
      n.injectDependencies(mockCache, cdl, null, config, mock(DistributionManager.class),
                           mock(InternalEntryFactory.class), mock(ClusterEventManager.class));
      cl = new PrimaryOwnerCacheListener();
      n.start();
      n.addListener(cl);
      ctx = new NonTxInvocationContext(null);
   }

   private static class MockCDL implements ClusteringDependentLogic {
      private static final TestAddress PRIMARY = new TestAddress(0);
      private static final TestAddress BACKUP = new TestAddress(1);
      private static final TestAddress NON_OWNER = new TestAddress(2);
      boolean isOwner, isPrimaryOwner;

      @Override
      public LocalizedCacheTopology getCacheTopology() {
         List<Address> members = Arrays.asList(PRIMARY, BACKUP, NON_OWNER);
         List<Address>[] ownership = new List[]{Arrays.asList(PRIMARY, BACKUP)};
         ConsistentHash ch = new DefaultConsistentHash(MurmurHash3.getInstance(), 2, 1, members, null, ownership);
         CacheTopology cacheTopology = new CacheTopology(0, 0, ch, null, CacheTopology.Phase.NO_REBALANCE, null, null);
         Address localAddress = isPrimaryOwner ? PRIMARY : (isOwner ? BACKUP : NON_OWNER);
         return new LocalizedCacheTopology(CacheMode.DIST_SYNC, cacheTopology, key -> 0, localAddress);
      }

      @Override
      public void commitEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx,
                              Flag trackFlag, boolean l1Invalidation) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, Object key, boolean removed) {
         return isOwner ? Commit.COMMIT_LOCAL : Commit.NO_COMMIT;
      }

      @Override
      public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Address getAddress() {
         throw new UnsupportedOperationException();
      }
   }

   public void testOwnership() {
      // Is not owner nor primary owner
      cdl.isOwner = false;
      cdl.isPrimaryOwner = false;
      n.notifyCacheEntryCreated("reject", "v1", null, true, ctx, null);
      n.notifyCacheEntryCreated("reject", "v1", null, false, ctx, null);

      assert !cl.isReceivedPost();
      assert !cl.isReceivedPre();
      assert cl.getInvocationCount() == 0;

      // Is an owner but not primary owner
      cdl.isOwner = true;
      cdl.isPrimaryOwner = false;
      n.notifyCacheEntryCreated("reject", "v1", null, true, ctx, null);
      n.notifyCacheEntryCreated("reject", "v1", null, false, ctx, null);

      assert !cl.isReceivedPost();
      assert !cl.isReceivedPre();
      assert cl.getInvocationCount() == 0;

      // Is primary owner
      cdl.isOwner = true;
      cdl.isPrimaryOwner = true;
      n.notifyCacheEntryCreated("accept", "v1", null, true, ctx, null);
      n.notifyCacheEntryCreated("accept", "v1", null, false, ctx, null);

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
