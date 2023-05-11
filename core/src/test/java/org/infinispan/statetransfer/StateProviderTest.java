package org.infinispan.statetransfer;

import static org.infinispan.context.Flag.STATE_TRANSFER_PROGRESS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.Notifications;
import org.infinispan.reactive.publisher.impl.SegmentAwarePublisherSupplier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Test for StateProviderImpl.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateProviderTest")
public class StateProviderTest {
   private static final Log log = LogFactory.getLog(StateProviderTest.class);

   // Number of segments must be a power of 2 for keyPartition4Segments to work
   private static final int NUM_SEGMENTS = 4;
   private static final TestAddress A = new TestAddress(0, "A");
   private static final TestAddress B = new TestAddress(1, "B");
   private static final TestAddress C = new TestAddress(2, "C");
   private static final TestAddress D = new TestAddress(3, "D");
   private static final TestAddress E = new TestAddress(4, "E");
   private static final TestAddress F = new TestAddress(5, "F");
   private static final TestAddress G = new TestAddress(6, "G");

   private static final PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
   static {
      Arrays.asList(A, B, C, D, E, F, G).forEach(address -> persistentUUIDManager.addPersistentAddressMapping(address, PersistentUUID.randomUUID()));
   }

   private Configuration configuration;

   private Cache cache;

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private ClusterCacheNotifier cacheNotifier;
   private PersistenceManager persistenceManager;
   private InternalDataContainer dataContainer;
   private TransactionTable transactionTable;
   private StateTransferLock stateTransferLock;
   private DistributionManager distributionManager;
   private LocalizedCacheTopology cacheTopology;
   private InternalEntryFactory ef;
   private LocalPublisherManager<?, ?> lpm;

   @BeforeMethod
   public void setUp() {
      // create cache configuration
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().timeout(30000)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      configuration = cb.build();

      cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      rpcManager = mock(RpcManager.class);
      commandsFactory = mock(CommandsFactory.class);
      cacheNotifier = mock(ClusterCacheNotifier.class);
      persistenceManager = mock(PersistenceManager.class);
      dataContainer = mock(InternalDataContainer.class);
      transactionTable = mock(TransactionTable.class);
      stateTransferLock = mock(StateTransferLock.class);
      distributionManager = mock(DistributionManager.class);
      ef = mock(InternalEntryFactory.class);
      lpm = mock(LocalPublisherManager.class);
      when(distributionManager.getCacheTopology()).thenAnswer(invocation -> cacheTopology);
   }

   public void test1() {
      // create list of 6 members
      List<Address> members1 = Arrays.asList(A, B, C, D, E, F);
      List<Address> members2 = new ArrayList<>(members1);
      members2.remove(A);
      members2.remove(F);
      members2.add(G);

      // create CHes
      KeyPartitioner keyPartitioner = new HashFunctionPartitioner(StateProviderTest.NUM_SEGMENTS);
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(2, StateProviderTest.NUM_SEGMENTS, members1, null);
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);

      // create dependencies
      when(rpcManager.getAddress()).thenReturn(A);
      when(rpcManager.invokeCommand(any(Address.class), any(), any(), any())).thenReturn(new CompletableFuture<>());

      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl();
      TestingUtil.inject(stateProvider, configuration, rpcManager, commandsFactory, cacheNotifier, persistenceManager,
                         dataContainer, transactionTable, stateTransferLock, distributionManager, ef, lpm, keyPartitioner,
                         TransactionOriginatorChecker.LOCAL);
      stateProvider.start();

      final List<InternalCacheEntry> cacheEntries = new ArrayList<>();
      Object key1 = new TestKey("key1", 0, keyPartitioner);
      Object key2 = new TestKey("key2", 0, keyPartitioner);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      when(dataContainer.iterator()).thenAnswer(invocation -> cacheEntries.iterator());
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.emptyList());

      CacheTopology simpleTopology = new CacheTopology(1, 1, ch1, ch1, ch1,
                                                       CacheTopology.Phase.READ_OLD_WRITE_ALL, ch1.getMembers(),
                                                       persistentUUIDManager.mapAddresses(ch1.getMembers()));
      this.cacheTopology = new LocalizedCacheTopology(CacheMode.DIST_SYNC, simpleTopology, keyPartitioner, A, true);
      stateProvider.onTopologyUpdate(this.cacheTopology, false);

      log.debug("ch1: " + ch1);
      IntSet segmentsToRequest = IntSets.from(ch1.getSegmentsForOwner(members1.get(0)));
      CompletionStage<List<TransactionInfo>> transactionsStage =
         stateProvider.getTransactionsForSegments(members1.get(0), 1, segmentsToRequest);
      List<TransactionInfo> transactions = CompletionStages.join(transactionsStage);
      assertEquals(0, transactions.size());

      CompletionStage<List<TransactionInfo>> transactionsStage2 =
         stateProvider.getTransactionsForSegments(members1.get(0), 1,
                                                  SmallIntSet.of(2, StateProviderTest.NUM_SEGMENTS));
      Exceptions.expectExecutionException(IllegalArgumentException.class, transactionsStage2.toCompletableFuture());

      verifyNoMoreInteractions(stateTransferLock);

      when(dataContainer.iterator(any())).thenReturn(cacheEntries.iterator());
      when(persistenceManager.publishEntries(any(IntSet.class), any(), anyBoolean(), anyBoolean(), any()))
         .thenReturn(Flowable.empty());
      SegmentAwarePublisherSupplier<?> supplier = mock(SegmentAwarePublisherSupplier.class);
      when(lpm.entryPublisher(any(), any(), any(), eq(
            EnumUtil.bitSetOf(STATE_TRANSFER_PROGRESS)), any(), any()))
            .thenAnswer(i -> supplier);
      List<SegmentAwarePublisherSupplier.NotificationWithLost<?>> values = cacheEntries.stream()
            .map(ice -> Notifications.value(ice, 0))
            .collect(Collectors.toList());
      values.add(Notifications.segmentComplete(0));
      when(supplier.publisherWithSegments())
            .thenAnswer(i -> Flowable.fromIterable(values));

      stateProvider.startOutboundTransfer(F, 1, IntSets.immutableSet(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      log.debug("ch2: " + ch2);
      simpleTopology = new CacheTopology(2, 1, ch2, ch2, ch2, CacheTopology.Phase.READ_OLD_WRITE_ALL,
                                                      ch2.getMembers(),
                                                      persistentUUIDManager.mapAddresses(ch2.getMembers()));
      this.cacheTopology = new LocalizedCacheTopology(CacheMode.DIST_SYNC, simpleTopology, keyPartitioner, A, true);
      stateProvider.onTopologyUpdate(this.cacheTopology, true);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.startOutboundTransfer(D, 1, IntSets.immutableSet(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.stop();

      assertFalse(stateProvider.isStateTransferInProgress());
   }

   public void test2() {
      // create list of 6 members
      List<Address> members1 = Arrays.asList(A, B, C, D, E, F);
      List<Address> members2 = new ArrayList<>(members1);
      members2.remove(A);
      members2.remove(F);
      members2.add(G);

      // create CHes
      KeyPartitioner keyPartitioner = new HashFunctionPartitioner(StateProviderTest.NUM_SEGMENTS);
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(2, NUM_SEGMENTS, members1, null);
      //todo [anistor] it seems that address 6 is not used for un-owned segments
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);

      // set up dependencies
      when(commandsFactory.buildStateResponseCommand(anyInt(), any(), anyBoolean()))
            .thenAnswer(invocation -> new StateResponseCommand(ByteString.fromString("testCache"),
                                                               (Integer) invocation.getArguments()[0],
                                                               (Collection<StateChunk>) invocation.getArguments()[1],
                                                               true));

      when(rpcManager.getAddress()).thenReturn(A);
      when(rpcManager.invokeCommand(any(Address.class), any(), any(), any())).thenReturn(new CompletableFuture<>());

      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl();
      TestingUtil.inject(stateProvider, configuration, rpcManager, commandsFactory, cacheNotifier, persistenceManager,
                         dataContainer, transactionTable, stateTransferLock, distributionManager, ef, lpm, keyPartitioner,
                         TransactionOriginatorChecker.LOCAL);
      stateProvider.start();

      final List<InternalCacheEntry> cacheEntries = new ArrayList<>();
      Object key1 = new TestKey("key1", 0, keyPartitioner);
      Object key2 = new TestKey("key2", 0, keyPartitioner);
      Object key3 = new TestKey("key3", 1, keyPartitioner);
      Object key4 = new TestKey("key4", 1, keyPartitioner);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      cacheEntries.add(new ImmortalCacheEntry(key3, "value3"));
      cacheEntries.add(new ImmortalCacheEntry(key4, "value4"));
      when(dataContainer.iterator(any())).thenReturn(cacheEntries.iterator());
      when(persistenceManager.publishEntries(any(IntSet.class), any(), anyBoolean(), anyBoolean(), any()))
         .thenReturn(Flowable.empty());

      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.emptyList());

      CacheTopology simpleTopology = new CacheTopology(1, 1, ch1, ch1, ch1, CacheTopology.Phase.READ_OLD_WRITE_ALL,
                                                      ch1.getMembers(),
                                                      persistentUUIDManager.mapAddresses(ch1.getMembers()));
      this.cacheTopology = new LocalizedCacheTopology(CacheMode.DIST_SYNC, simpleTopology, keyPartitioner, A, true);
      stateProvider.onTopologyUpdate(this.cacheTopology, false);

      log.debug("ch1: " + ch1);
      IntSet segmentsToRequest = IntSets.from(ch1.getSegmentsForOwner(members1.get(0)));
      CompletionStage<List<TransactionInfo>> transactionsStage =
         stateProvider.getTransactionsForSegments(members1.get(0), 1, segmentsToRequest);
      List<TransactionInfo> transactions = CompletionStages.join(transactionsStage);
      assertEquals(0, transactions.size());

      CompletionStage<List<TransactionInfo>> transactionsStage2 =
         stateProvider.getTransactionsForSegments(members1.get(0), 1,
                                                  SmallIntSet.of(2, StateProviderTest.NUM_SEGMENTS));
      Exceptions.expectExecutionException(IllegalArgumentException.class, transactionsStage2.toCompletableFuture());

      verifyNoMoreInteractions(stateTransferLock);

      SegmentAwarePublisherSupplier<?> supplier = mock(SegmentAwarePublisherSupplier.class);
      when(lpm.entryPublisher(any(), any(), any(),
            eq(EnumUtil.bitSetOf(STATE_TRANSFER_PROGRESS)), any(), any()))
            .thenAnswer(i -> supplier);
      List<SegmentAwarePublisherSupplier.NotificationWithLost<?>> values = cacheEntries.stream()
            .map(ice -> Notifications.value(ice, 0))
            .collect(Collectors.toList());
      values.add(Notifications.segmentComplete(0));
      when(supplier.publisherWithSegments())
            .thenAnswer(i -> Flowable.fromIterable(values));
      stateProvider.startOutboundTransfer(F, 1, IntSets.immutableSet(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      // TestingUtil.sleepThread(15000);
      log.debug("ch2: " + ch2);
      simpleTopology = new CacheTopology(2, 1, ch2, ch2, ch2, CacheTopology.Phase.READ_OLD_WRITE_ALL,
                                                      ch2.getMembers(),
                                                      persistentUUIDManager.mapAddresses(ch2.getMembers()));
      this.cacheTopology = new LocalizedCacheTopology(CacheMode.DIST_SYNC, simpleTopology, keyPartitioner, A, true);
      stateProvider.onTopologyUpdate(this.cacheTopology, false);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.startOutboundTransfer(E, 1, IntSets.immutableSet(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.stop();

      assertFalse(stateProvider.isStateTransferInProgress());
   }
}
