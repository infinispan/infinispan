package org.infinispan.notifications.cachelistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.cache.impl.EncoderCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.MockBasicComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.cluster.ClusterEventManager;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.BlockingManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(testName = "notifications.cachelistener.OnlyPrimaryOwnerTest", groups = "unit")
public class OnlyPrimaryOwnerTest {
   CacheNotifierImpl n;
   EncoderCache mockCache;
   PrimaryOwnerCacheListener cl;
   InvocationContext ctx;
   MockCDL cdl = new MockCDL();

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
      mockRegistry.registerMocks(RpcManager.class, CommandsFactory.class, Encoder.class);
      mockRegistry.registerMock(KnownComponentNames.INTERNAL_MARSHALLER, Marshaller.class);
      Configuration config = new ConfigurationBuilder().memory().storageType(StorageType.OBJECT).build();
      ClusterEventManager cem = mock(ClusterEventManager.class);
      when(cem.sendEvents(any())).thenReturn(CompletableFutures.completedNull());
      TestingUtil.inject(n, mockCache, cdl, config, mockRegistry,
                         mock(InternalEntryFactory.class), cem, mock(KeyPartitioner.class), mock(BlockingManager.class));
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
         ConsistentHash ch = new DefaultConsistentHash(2, 1, members, null, ownership);
         CacheTopology cacheTopology = new CacheTopology(0, 0, ch, null, CacheTopology.Phase.NO_REBALANCE, null, null);
         Address localAddress = isPrimaryOwner ? PRIMARY : (isOwner ? BACKUP : NON_OWNER);
         return new LocalizedCacheTopology(CacheMode.DIST_SYNC, cacheTopology, key -> 0, localAddress, true);
      }

      @Override
      public CompletionStage<Void> commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx,
                              Flag trackFlag, boolean l1Invalidation) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
         return isOwner ? Commit.COMMIT_LOCAL : Commit.NO_COMMIT;
      }

      @Override
      public CompletionStage<Map<Object, IncrementableEntryVersion>> createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
         throw new UnsupportedOperationException();
      }

      @Override
      public Address getAddress() {
         throw new UnsupportedOperationException();
      }

      @Override
      public <K, V> EntryLoader<K, V> getEntryLoader() {
         throw new UnsupportedOperationException();
      }

      @Override
      public void start() {

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
