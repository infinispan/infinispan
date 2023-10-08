package org.infinispan.statetransfer;

import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.SingleKeyNonTxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
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
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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
   private static final ByteString CACHE_NAME = ByteString.fromString("test-cache");

   private ExecutorService pooledExecutorService;

   @BeforeMethod
   public void createExecutorService() {
      pooledExecutorService = new ThreadPoolExecutor(0, 20, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            getTestThreadFactory("Worker"),
            new ThreadPoolExecutor.CallerRunsPolicy());
   }

   @AfterMethod
   public void shutdownExecutorService() {
      if (pooledExecutorService != null) {
         pooledExecutorService.shutdownNow();
         pooledExecutorService = null;
      }
   }

   // creates 4 members
   private static Address[] createMembers(PersistentUUIDManager persistentUUIDManager) {
      Address[] addresses = new Address[4];
      for (int i = 0; i < 4; i++) {
         addresses[i] = new TestAddress(i);
         persistentUUIDManager.addPersistentAddressMapping(addresses[i], PersistentUUID.randomUUID());
      }
      return addresses;
   }

   private static XSiteStateTransferManager mockXSiteStateTransferManager() {
      XSiteStateTransferManager mock = mock(XSiteStateTransferManager.class);
      doNothing().when(mock).onTopologyUpdated(any(CacheTopology.class), anyBoolean());
      return mock;
   }

   private static CommandsFactory mockCommandsFactory() {
      CommandsFactory mock = mock(CommandsFactory.class);
      when(mock.buildStateTransferStartCommand(anyInt(), any(IntSet.class)))
            .thenAnswer(invocation -> new StateTransferStartCommand(CACHE_NAME, invocation.getArgument(0),
                  invocation.getArgument(1)));
      when(mock.buildStateTransferGetTransactionsCommand(anyInt(), any(IntSet.class)))
            .thenAnswer(invocation -> new StateTransferGetTransactionsCommand(CACHE_NAME, invocation.getArgument(0),
                  invocation.getArgument(1)));
      when(mock.buildStateTransferCancelCommand(anyInt(), any(IntSet.class)))
            .thenAnswer(invocation -> new StateTransferCancelCommand(CACHE_NAME, invocation.getArgument(0),
                  invocation.getArgument(1)));
      when(mock.buildPutKeyValueCommand(any(), any(), anyInt(), any(Metadata.class), anyLong()))
            .thenAnswer(invocation -> new PutKeyValueCommand(invocation.getArgument(0), invocation.getArgument(1),
                  false, false, invocation.getArgument(3), invocation.getArgument(2),
                  invocation.getArgument(4), CommandInvocationId.DUMMY_INVOCATION_ID));
      return mock;
   }

   private static Configuration createConfiguration() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().timeout(30000)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);

      return cb.build();
   }

   private static Cache<?, ?> mockCache() {
      Cache<?, ?> cache = mock(Cache.class);
      when(cache.getName()).thenReturn(CACHE_NAME.toString());
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      return cache;
   }

   private static RpcManager mockRpcManager(Map<Address, Set<Integer>> requestedSegments, Set<Integer> flatRequestedSegments, Address address) {
      Transport transport = mock(Transport.class);
      when(transport.getViewId()).thenReturn(1);

      RpcManager rpcManager = mock(RpcManager.class);
      Answer<?> successfulResponse = invocation -> CompletableFuture.completedFuture(
            SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);

      when(rpcManager.invokeCommand(any(Address.class), any(StateTransferGetTransactionsCommand.class),
            any(ResponseCollector.class),
            any(RpcOptions.class)))
            .thenAnswer(invocation -> {
               Address recipient = invocation.getArgument(0);
               StateTransferGetTransactionsCommand cmd = invocation.getArgument(1);
               Set<Integer> segments = cmd.getSegments();
               requestedSegments.put(recipient, segments);
               flatRequestedSegments.addAll(segments);
               return CompletableFuture.completedFuture(SuccessfulResponse.create(new ArrayList<TransactionInfo>()));
            });
      when(rpcManager.invokeCommand(any(Address.class), any(StateTransferStartCommand.class),
            any(ResponseCollector.class),
            any(RpcOptions.class)))
            .thenAnswer(successfulResponse);
      when(rpcManager.invokeCommand(any(Address.class), any(StateTransferCancelCommand.class),
            any(ResponseCollector.class),
            any(RpcOptions.class)))
            .thenAnswer(successfulResponse);
      when(rpcManager.getSyncRpcOptions()).thenReturn(new RpcOptions(DeliverOrder.NONE, 10000, TimeUnit.MILLISECONDS));
      when(rpcManager.blocking(any())).thenAnswer(invocation -> ((CompletionStage<?>) invocation
            .getArgument(0)).toCompletableFuture().join());
      when(rpcManager.getAddress()).thenReturn(address);
      when(rpcManager.getTransport()).thenReturn(transport);
      return rpcManager;
   }

   private static PersistenceManager mockPersistenceManager() {
      PersistenceManager persistenceManager = mock(PersistenceManager.class);
      when(persistenceManager.removeSegments(any())).thenReturn(CompletableFuture.completedFuture(false));
      when(persistenceManager.addSegments(any())).thenReturn(CompletableFuture.completedFuture(false));
      when(persistenceManager.publishKeys(any(), any())).thenReturn(Flowable.empty());
      return persistenceManager;
   }

   private static TransactionTable mockTransactionTable() {
      TransactionTable transactionTable = mock(TransactionTable.class);
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.emptyList());
      return transactionTable;
   }

   private static InvocationContextFactory mockInvocationContextFactory() {
      InvocationContextFactory icf = mock(InvocationContextFactory.class);
      when(icf.createSingleKeyNonTxInvocationContext()).thenAnswer(
            invocationOnMock -> new SingleKeyNonTxInvocationContext(null));
      return icf;
   }

   private static void noRebalance(StateConsumer stateConsumer, PersistentUUIDManager persistentUUIDManager, int topologyId, int rebalanceId, ConsistentHash ch) {
      stateConsumer.onTopologyUpdate(
            new CacheTopology(topologyId, rebalanceId, ch, null, CacheTopology.Phase.NO_REBALANCE,
                  ch.getMembers(), persistentUUIDManager.mapAddresses(ch.getMembers())), false);
   }

   private static void rebalanceStart(StateConsumer stateConsumer, PersistentUUIDManager persistentUUIDManager, int topologyId, int rebalanceId, ConsistentHash current, ConsistentHash pending, ConsistentHash union) {
      stateConsumer.onTopologyUpdate(
            new CacheTopology(topologyId, rebalanceId, current, pending, union, CacheTopology.Phase.READ_OLD_WRITE_ALL,
                  union.getMembers(), persistentUUIDManager.mapAddresses(union.getMembers())), true);
   }

   private static void assertRebalanceStart(StateConsumerImpl stateConsumer, ConsistentHash current, ConsistentHash pending, Address member, Set<Integer> flatRequestedSegments) {
      // check that all segments have been requested
      Set<Integer> oldSegments = current.getSegmentsForOwner(member);
      Set<Integer> newSegments = pending.getSegmentsForOwner(member);
      newSegments.removeAll(oldSegments);

      log.debugf("Rebalancing. Added segments=%s, old segments=%s", newSegments, oldSegments);

      assertTrue(stateConsumer.hasActiveTransfers());
      assertEquals(flatRequestedSegments, newSegments);
      assertEquals(stateConsumer.inflightRequestCount(), newSegments.size());
   }

   private static void completeAndCheckRebalance(StateConsumerImpl stateConsumer, Map<Address, Set<Integer>> requestedSegments, int topologyId) throws ExecutionException, InterruptedException, TimeoutException {
      // We count how many segments were requested and then start to apply the state individually, to assert that the
      // number of in-flight requests will decrease accordingly. During real usage, the state chunk collections can
      // have more than a single segment.
      long inflightCounter = requestedSegments.values().stream().mapToLong(Collection::size).sum();
      assertEquals(stateConsumer.inflightRequestCount(), inflightCounter);
      for (Map.Entry<Address, Set<Integer>> entry : requestedSegments.entrySet()) {
         for (Integer segment : entry.getValue()) {
            Collection<StateChunk> chunks = Collections.singletonList(
                  new StateChunk(segment, Collections.emptyList(), true));
            stateConsumer.applyState(entry.getKey(), topologyId, chunks)
                  .toCompletableFuture()
                  .get(10, TimeUnit.SECONDS);

            inflightCounter -= 1;
            assertEquals(stateConsumer.inflightRequestCount(), inflightCounter);
         }
      }
      assertEquals(stateConsumer.inflightRequestCount(), 0);
      eventually(() -> !stateConsumer.hasActiveTransfers());
   }

   private static void applyState(StateConsumer stateConsumer, Map<Address, Set<Integer>> requestedSegments, Collection<InternalCacheEntry<?, ?>> cacheEntries) {
      Map.Entry<Address, Set<Integer>> entry = requestedSegments.entrySet().iterator().next();
      Collection<StateChunk> chunks = Collections.singletonList(
            new StateChunk(entry.getValue().iterator().next(), cacheEntries, true));
      stateConsumer.applyState(entry.getKey(), 22, chunks);
   }

   private void injectComponents(StateConsumer stateConsumer, AsyncInterceptorChain interceptorChain, RpcManager rpcManager) {
      TestingUtil.inject(stateConsumer,
            mockCache(),
            TestingUtil.named(NON_BLOCKING_EXECUTOR, pooledExecutorService),
            interceptorChain,
            mockInvocationContextFactory(),
            createConfiguration(),
            rpcManager,
            mockCommandsFactory(),
            mockPersistenceManager(),
            mock(InternalDataContainer.class),
            mockTransactionTable(),
            mock(StateTransferLock.class),
            mock(CacheNotifier.class),
            new CommitManager(),
            new CommandAckCollector(),
            new HashFunctionPartitioner(),
            mock(InternalConflictManager.class),
            mock(DistributionManager.class),
            mock(LocalPublisherManager.class),
            mock(PerCacheInboundInvocationHandler.class),
            mockXSiteStateTransferManager());
   }

   public void testClusterRecoverDuringStateTransfer() throws Exception {
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();

      // create list of 4 members
      Address[] addresses = createMembers(persistentUUIDManager);
      List<Address> members1 = Arrays.asList(addresses[0], addresses[1], addresses[2], addresses[3]);
      List<Address> members2 = Arrays.asList(addresses[0], addresses[1], addresses[2]);

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(2, 40, members1, null);
      final DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);
      DefaultConsistentHash ch3 = chf.rebalance(ch2);
      DefaultConsistentHash ch23 = chf.union(ch2, ch3);

      log.debug(ch1);
      log.debug(ch2);

      final Map<Address, Set<Integer>> requestedSegments = new ConcurrentHashMap<>();
      final Set<Integer> flatRequestedSegments = new ConcurrentSkipListSet<>();

      // create state provider
      final StateConsumerImpl stateConsumer = new StateConsumerImpl();
      injectComponents(stateConsumer, mock(AsyncInterceptorChain.class),
            mockRpcManager(requestedSegments, flatRequestedSegments, addresses[0]));

      stateConsumer.start();
      assertFalse(stateConsumer.hasActiveTransfers());

      // node 4 leaves
      noRebalance(stateConsumer, persistentUUIDManager, 1, 1, ch2);
      assertFalse(stateConsumer.hasActiveTransfers());

      // start a rebalance
      rebalanceStart(stateConsumer, persistentUUIDManager, 2, 2, ch2, ch3, ch23);
      assertRebalanceStart(stateConsumer, ch2, ch3, addresses[0], flatRequestedSegments);

      // simulate a cluster state recovery and return to ch2
      Future<Object> future = fork(() -> {
         noRebalance(stateConsumer, persistentUUIDManager, 3, 2, ch2);
         return null;
      });
      noRebalance(stateConsumer, persistentUUIDManager, 3, 2, ch2);
      future.get();
      assertFalse(stateConsumer.hasActiveTransfers());


      // restart the rebalance
      requestedSegments.clear();
      flatRequestedSegments.clear();
      rebalanceStart(stateConsumer, persistentUUIDManager, 4, 4, ch2, ch3, ch23);
      assertRebalanceStart(stateConsumer, ch2, ch3, addresses[0], flatRequestedSegments);

      completeAndCheckRebalance(stateConsumer, requestedSegments, 4);
      stateConsumer.stop();
   }

   // Reproducer for ISPN-14982
   public void testJoinDuringStateTransfer() throws Exception {
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();

      // create list of 4 members
      Address[] addresses = createMembers(persistentUUIDManager);
      List<Address> members1 = Arrays.asList(addresses[0], addresses[1], addresses[2]);
      List<Address> members2 = Arrays.asList(addresses[1], addresses[2]);

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(2, 40, members1, null);
      final DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);
      DefaultConsistentHash ch3 = chf.rebalance(ch2);
      DefaultConsistentHash ch23 = chf.union(ch2, ch3);

      log.debug(ch1);
      log.debug(ch2);
      log.debug(ch23);

      final Map<Address, Set<Integer>> requestedSegments = new ConcurrentHashMap<>();
      final Set<Integer> flatRequestedSegments = new ConcurrentSkipListSet<>();
      final CompletableFuture<Object> putFuture = new CompletableFuture<>();

      // create dependencies
      AsyncInterceptorChain interceptorChain = mock(AsyncInterceptorChain.class);
      when(interceptorChain.invokeAsync(any(), any())).thenReturn(putFuture);

      // create state provider
      final StateConsumerImpl stateConsumer = new StateConsumerImpl();
      injectComponents(stateConsumer, interceptorChain,
            mockRpcManager(requestedSegments, flatRequestedSegments, addresses[1]));
      stateConsumer.start();

      // initial topology
      noRebalance(stateConsumer, persistentUUIDManager, 21, 7, ch1);
      assertFalse(stateConsumer.hasActiveTransfers());

      // start a rebalance (copied form logs)
      //CacheTopology{id=22, phase=READ_OLD_WRITE_ALL, rebalanceId=8, currentCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 29+10, node-5: 31+10]}, pendingCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 30+30, node-5: 30+30]}, unionCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 29+31, node-5: 31+29]}, actualMembers=[node-3, node-5], persistentUUIDs=...}
      rebalanceStart(stateConsumer, persistentUUIDManager, 22, 8, ch2, ch3, ch23);
      assertRebalanceStart(stateConsumer, ch2, ch3, addresses[1], flatRequestedSegments);

      applyState(stateConsumer, requestedSegments, Collections.singletonList(new ImmortalCacheEntry("a", "b")));

      // merge view update
      //CacheTopology{id=23, phase=NO_REBALANCE, rebalanceId=9, currentCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 29+10, node-5: 31+10]}, pendingCH=null, unionCH=null, actualMembers=[node-3, node-5], persistentUUIDs=...}
      noRebalance(stateConsumer, persistentUUIDManager, 23, 9, ch2);

      // restart the rebalance
      requestedSegments.clear();
      flatRequestedSegments.clear();

      // CacheTopology{id=24, phase=READ_OLD_WRITE_ALL, rebalanceId=10, currentCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 29+10, node-5: 31+10]}, pendingCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 30+30, node-5: 30+30]}, unionCH=DefaultConsistentHash{ns=60, owners = (2)[node-3: 29+31, node-5: 31+29]}, actualMembers=[node-3, node-5], persistentUUIDs=...}
      rebalanceStart(stateConsumer, persistentUUIDManager, 24, 10, ch2, ch3, ch23);
      assertRebalanceStart(stateConsumer, ch2, ch3, addresses[1], flatRequestedSegments);

      // let the apply state complete
      putFuture.complete(null);

      completeAndCheckRebalance(stateConsumer, requestedSegments, 24);

      stateConsumer.stop();
   }
}
