package org.infinispan.statetransfer;

import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Tests StateConsumerImpl.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateConsumerTest")
public class StateConsumerTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(StateConsumerTest.class);

   private ExecutorService pooledExecutorService;

   @AfterMethod
   public void tearDown() {
      if (pooledExecutorService != null) {
         pooledExecutorService.shutdownNow();
      }
   }

   public void test1() throws Exception {
      // create cache configuration
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().timeout(30000)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);

      Configuration configuration = cb.build();
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();

      // create list of 6 members
      Address[] addresses = new Address[4];
      for (int i = 0; i < 4; i++) {
         addresses[i] = new TestAddress(i);
         persistentUUIDManager.addPersistentAddressMapping(addresses[i], PersistentUUID.randomUUID());
      }
      List<Address> members1 = Arrays.asList(addresses[0], addresses[1], addresses[2], addresses[3]);
      List<Address> members2 = Arrays.asList(addresses[0], addresses[1], addresses[2]);

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      KeyPartitioner keyPartitioner = new HashFunctionPartitioner(40);
      DefaultConsistentHash ch1 = chf.create(2, 40, members1, null);
      final DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);
      DefaultConsistentHash ch3 = chf.rebalance(ch2);
      DefaultConsistentHash ch23 = chf.union(ch2, ch3);

      log.debug(ch1);
      log.debug(ch2);

      // create dependencies
      Cache cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);

      pooledExecutorService = new ThreadPoolExecutor(0, 20, 0L,
                                                     TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                                                     getTestThreadFactory("Worker"),
                                                     new ThreadPoolExecutor.CallerRunsPolicy());

      LocalTopologyManager localTopologyManager = mock(LocalTopologyManager.class);
      CacheNotifier cacheNotifier = mock(CacheNotifier.class);
      RpcManager rpcManager = mock(RpcManager.class);
      Transport transport = mock(Transport.class);
      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      PersistenceManager persistenceManager = mock(PersistenceManager.class);
      InternalDataContainer dataContainer = mock(InternalDataContainer.class);
      TransactionTable transactionTable = mock(TransactionTable.class);
      StateTransferLock stateTransferLock = mock(StateTransferLock.class);
      AsyncInterceptorChain interceptorChain = mock(AsyncInterceptorChain.class);
      InvocationContextFactory icf = mock(InvocationContextFactory.class);
      InternalConflictManager conflictManager = mock(InternalConflictManager.class);
      DistributionManager distributionManager = mock(DistributionManager.class);
      LocalPublisherManager localPublisherManager = mock(LocalPublisherManager.class);
      PerCacheInboundInvocationHandler invocationHandler = mock(PerCacheInboundInvocationHandler.class);
      XSiteStateTransferManager xSiteStateTransferManager = mock(XSiteStateTransferManager.class);

      when(persistenceManager.removeSegments(any())).thenReturn(CompletableFuture.completedFuture(false));
      when(persistenceManager.addSegments(any())).thenReturn(CompletableFuture.completedFuture(false));
      when(persistenceManager.publishKeys(any(), any())).thenReturn(Flowable.empty());

      when(commandsFactory.buildStateTransferStartCommand(anyInt(), any(IntSet.class)))
            .thenAnswer(invocation -> new StateTransferStartCommand(ByteString.fromString("cache1"),
                  (Integer) invocation.getArguments()[0],
                  (IntSet) invocation.getArguments()[1]));

      when(commandsFactory.buildStateTransferGetTransactionsCommand(anyInt(), any(IntSet.class)))
            .thenAnswer(invocation -> new StateTransferGetTransactionsCommand(ByteString.fromString("cache1"),
                  (Integer) invocation.getArguments()[0],
                  (IntSet) invocation.getArguments()[1]));

      when(commandsFactory.buildStateTransferCancelCommand(anyInt(), any(IntSet.class)))
            .thenAnswer(invocation -> new StateTransferCancelCommand(ByteString.fromString("cache1"),
                  (Integer) invocation.getArguments()[0],
                  (IntSet) invocation.getArguments()[1]));

      when(transport.getViewId()).thenReturn(1);
      when(rpcManager.getAddress()).thenReturn(addresses[0]);
      when(rpcManager.getTransport()).thenReturn(transport);

      final Map<Address, Set<Integer>> requestedSegments = new ConcurrentHashMap<>();
      final Set<Integer> flatRequestedSegments = new ConcurrentSkipListSet<>();
      when(rpcManager.invokeCommand(any(Address.class), any(StateTransferGetTransactionsCommand.class), any(ResponseCollector.class),
            any(RpcOptions.class)))
            .thenAnswer(invocation -> {
               Address recipient = invocation.getArgument(0);
               StateTransferGetTransactionsCommand cmd = invocation.getArgument(1);
               Set<Integer> segments = cmd.getSegments();
               requestedSegments.put(recipient, segments);
               flatRequestedSegments.addAll(segments);
               return CompletableFuture.completedFuture(SuccessfulResponse.create(new ArrayList<TransactionInfo>()));
            });
      Answer<?> successfulResponse = invocation -> CompletableFuture.completedFuture(SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
      when(rpcManager.invokeCommand(any(Address.class), any(StateTransferStartCommand.class), any(ResponseCollector.class),
            any(RpcOptions.class)))
            .thenAnswer(successfulResponse);

      when(rpcManager.invokeCommand(any(Address.class), any(StateTransferCancelCommand.class), any(ResponseCollector.class),
            any(RpcOptions.class)))
            .thenAnswer(successfulResponse);


      when(rpcManager.getSyncRpcOptions()).thenReturn(new RpcOptions(DeliverOrder.NONE, 10000, TimeUnit.MILLISECONDS));
      when(rpcManager.blocking(any(CompletionStage.class))).thenAnswer(invocation -> ((CompletionStage) invocation
            .getArgument(0)).toCompletableFuture().join());
      doNothing().when(xSiteStateTransferManager).onTopologyUpdated(any(CacheTopology.class), anyBoolean());

      // create state provider
      final StateConsumerImpl stateConsumer = new StateConsumerImpl();
      TestingUtil.inject(stateConsumer, cache, TestingUtil.named(NON_BLOCKING_EXECUTOR, pooledExecutorService),
                         localTopologyManager, interceptorChain, icf, configuration, rpcManager,
                         commandsFactory, persistenceManager, dataContainer, transactionTable, stateTransferLock, cacheNotifier,
                         new CommitManager(), new CommandAckCollector(), new TriangleOrderManager(0),
                         new HashFunctionPartitioner(), conflictManager, distributionManager, localPublisherManager,
                         invocationHandler, xSiteStateTransferManager);
      stateConsumer.start();

      final List<InternalCacheEntry> cacheEntries = new ArrayList<>();
      Object key1 = new TestKey("key1", 0, keyPartitioner);
      Object key2 = new TestKey("key2", 0, keyPartitioner);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      when(dataContainer.iterator()).thenAnswer(invocation -> cacheEntries.iterator());
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.emptyList());

      assertFalse(stateConsumer.hasActiveTransfers());

      // node 4 leaves
      stateConsumer.onTopologyUpdate(new CacheTopology(1, 1, ch2, null, CacheTopology.Phase.NO_REBALANCE, ch2.getMembers(), persistentUUIDManager.mapAddresses(ch2.getMembers())), false);
      assertFalse(stateConsumer.hasActiveTransfers());

      // start a rebalance
      stateConsumer.onTopologyUpdate(new CacheTopology(2, 2, ch2, ch3, ch23, CacheTopology.Phase.READ_OLD_WRITE_ALL,
            ch23.getMembers(), persistentUUIDManager.mapAddresses(ch23.getMembers())), true);
      assertTrue(stateConsumer.hasActiveTransfers());

      // check that all segments have been requested
      Set<Integer> oldSegments = ch2.getSegmentsForOwner(addresses[0]);
      final Set<Integer> newSegments = ch3.getSegmentsForOwner(addresses[0]);
      newSegments.removeAll(oldSegments);
      log.debugf("Rebalancing. Added segments=%s, old segments=%s", newSegments, oldSegments);
      assertEquals(flatRequestedSegments, newSegments);
      assertEquals(stateConsumer.inflightRequestCount(), newSegments.size());

      // simulate a cluster state recovery and return to ch2
      Future<Object> future = fork(() -> {
         stateConsumer.onTopologyUpdate(new CacheTopology(3, 2, ch2, null, CacheTopology.Phase.NO_REBALANCE, ch2.getMembers(), persistentUUIDManager.mapAddresses(ch2.getMembers())), false);
         return null;
      });
      stateConsumer.onTopologyUpdate(new CacheTopology(3, 2, ch2, null, CacheTopology.Phase.NO_REBALANCE, ch2.getMembers(), persistentUUIDManager.mapAddresses(ch2.getMembers())), false);
      future.get();
      assertFalse(stateConsumer.hasActiveTransfers());


      // restart the rebalance
      requestedSegments.clear();
      stateConsumer.onTopologyUpdate(new CacheTopology(4, 4, ch2, ch3, ch23, CacheTopology.Phase.READ_OLD_WRITE_ALL,
            ch23.getMembers(), persistentUUIDManager.mapAddresses(ch23.getMembers())), true);
      assertTrue(stateConsumer.hasActiveTransfers());
      assertEquals(flatRequestedSegments, newSegments);

      // We count how many segments were requested and then start to apply the state individually, to assert that the
      // number of in-flight requests will decrease accordingly. During real usage, the state chunk collections can
      // have more than a single segment.
      long inflightCounter = requestedSegments.values().stream().mapToLong(Collection::size).sum();
      assertEquals(stateConsumer.inflightRequestCount(), inflightCounter);
      for (Map.Entry<Address, Set<Integer>> entry : requestedSegments.entrySet()) {
         for (Integer segment : entry.getValue()) {
            Collection<StateChunk> chunks = Collections.singletonList(new StateChunk(segment, Collections.emptyList(), true));
            stateConsumer.applyState(entry.getKey(), 4, chunks)
                  .toCompletableFuture()
                  .get(10, TimeUnit.SECONDS);

            inflightCounter -= 1;
            assertEquals(stateConsumer.inflightRequestCount(), inflightCounter);
         }
      }

      stateConsumer.stop();
      assertFalse(stateConsumer.hasActiveTransfers());
      assertEquals(stateConsumer.inflightRequestCount(), 0);
   }
}
