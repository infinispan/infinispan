package org.infinispan.statetransfer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.DefaultConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.notifications.cachelistener.cluster.ClusterCacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.infinispan.transaction.impl.TransactionOriginatorChecker;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for StateProviderImpl.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateProviderTest")
public class StateProviderTest {

   private static final Log log = LogFactory.getLog(StateProviderTest.class);
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

   private ExecutorService mockExecutorService;
   private Cache cache;

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private ClusterCacheNotifier cacheNotifier;
   private PersistenceManager persistenceManager;
   private DataContainer dataContainer;
   private TransactionTable transactionTable;
   private StateTransferLock stateTransferLock;
   private StateConsumer stateConsumer;
   private CacheTopology cacheTopology;
   private InternalEntryFactory ef;

   @BeforeClass
   public void setUp() {
      // create cache configuration
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().timeout(10000)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      configuration = cb.build();

      mockExecutorService = mock(ExecutorService.class);
      cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      rpcManager = mock(RpcManager.class);
      commandsFactory = mock(CommandsFactory.class);
      cacheNotifier = mock(ClusterCacheNotifier.class);
      persistenceManager = mock(PersistenceManager.class);
      dataContainer = mock(DataContainer.class);
      transactionTable = mock(TransactionTable.class);
      stateTransferLock = mock(StateTransferLock.class);
      stateConsumer = mock(StateConsumer.class);
      ef = mock(InternalEntryFactory.class);
      when(stateConsumer.getCacheTopology()).thenAnswer(invocation -> cacheTopology);
   }

   public void test1() throws InterruptedException {
      int numSegments = 4;

      // create list of 6 members
      List<Address> members1 = Arrays.asList(A, B, C, D, E, F);
      List<Address> members2 = new ArrayList<>(members1);
      members2.remove(A);
      members2.remove(F);
      members2.add(G);

      // create CHes
      KeyPartitioner keyPartitioner = new HashFunctionPartitioner();
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(MurmurHash3.getInstance(), 2, numSegments, members1, null);
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);

      // create dependencies
      when(mockExecutorService.submit(any(Runnable.class)))
            .thenAnswer((Answer<Future<?>>) invocation -> null);

      when(rpcManager.getAddress()).thenReturn(A);
      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class))).thenAnswer(invocation -> {
         Object[] args = invocation.getArguments();
         return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], DeliverOrder.PER_SENDER);
      });

      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl();
      stateProvider.init(cache, mockExecutorService,
            configuration, rpcManager, commandsFactory, cacheNotifier, persistenceManager,
            dataContainer, transactionTable, stateTransferLock, stateConsumer, ef, keyPartitioner, TransactionOriginatorChecker.LOCAL);

      final List<InternalCacheEntry> cacheEntries = new ArrayList<>();
      Object key1 = new TestKey("key1", 0, ch1);
      Object key2 = new TestKey("key2", 0, ch1);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      when(dataContainer.iterator()).thenAnswer(invocation -> cacheEntries.iterator());
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.emptyList());

      cacheTopology = new CacheTopology(1, 1, ch1, ch1, ch1, CacheTopology.Phase.READ_OLD_WRITE_ALL, ch1.getMembers(), persistentUUIDManager.mapAddresses(ch1.getMembers()));
      stateProvider.onTopologyUpdate(cacheTopology, false);

      log.debug("ch1: " + ch1);
      Set<Integer> segmentsToRequest = ch1.getSegmentsForOwner(members1.get(0));
      List<TransactionInfo> transactions = stateProvider.getTransactionsForSegments(members1.get(0), 1, segmentsToRequest);
      assertEquals(0, transactions.size());

      try {
         stateProvider.getTransactionsForSegments(members1.get(0), 1, SmallIntSet.of(2, numSegments));
         fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
         // expected
      }

      verifyNoMoreInteractions(stateTransferLock);

      stateProvider.startOutboundTransfer(F, 1, Collections.singleton(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      log.debug("ch2: " + ch2);
      cacheTopology = new CacheTopology(2, 1, ch2, ch2, ch2, CacheTopology.Phase.READ_OLD_WRITE_ALL, ch2.getMembers(), persistentUUIDManager.mapAddresses(ch2.getMembers()));
      stateProvider.onTopologyUpdate(cacheTopology, true);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.startOutboundTransfer(D, 1, Collections.singleton(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.stop();

      assertFalse(stateProvider.isStateTransferInProgress());
   }

   public void test2() throws InterruptedException {
      int numSegments = 4;

      // create list of 6 members
      List<Address> members1 = Arrays.asList(A, B, C, D, E, F);
      List<Address> members2 = new ArrayList<>(members1);
      members2.remove(A);
      members2.remove(F);
      members2.add(G);

      // create CHes
      KeyPartitioner keyPartitioner = new HashFunctionPartitioner();
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(MurmurHash3.getInstance(), 2, numSegments, members1, null);
      //todo [anistor] it seems that address 6 is not used for un-owned segments
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2, null);

      when(commandsFactory.buildStateResponseCommand(any(Address.class), anyInt(), any(Collection.class), anyBoolean(), anyBoolean()))
            .thenAnswer(invocation -> new StateResponseCommand(ByteString.fromString("testCache"),
                                                               (Address) invocation.getArguments()[0],
                                                               (Integer) invocation.getArguments()[1],
                                                               (Collection<StateChunk>) invocation.getArguments()[2],
                                                               true, false));

      // create dependencies
      when(rpcManager.getAddress()).thenReturn(A);

      //rpcManager.invokeRemotelyInFuture(Collections.singleton(destination), cmd, false, sendFuture, timeout);
//      doAnswer(new Answer<Map<Address, Response>>() {
//         @Override
//         public Map<Address, Response> answer(InvocationOnMock invocation) {
//            Collection<Address> recipients = (Collection<Address>) invocation.getArguments()[0];
//            ReplicableCommand rpcCommand = (ReplicableCommand) invocation.getArguments()[1];
//            if (rpcCommand instanceof StateResponseCommand) {
//               Map<Address, Response> results = new HashMap<Address, Response>();
//               TestingUtil.sleepThread(10000, "RpcManager mock interrupted during invokeRemotelyInFuture(..)");
//               return results;
//            }
//            return Collections.emptyMap();
//         }
//      }).when(rpcManager).invokeRemotelyInFuture(any(Collection.class), any(ReplicableCommand.class), any(RpcOptions.class),
//                                                 any(NotifyingNotifiableFuture.class));

      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class))).thenAnswer(invocation -> {
         Object[] args = invocation.getArguments();
         return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], DeliverOrder.PER_SENDER);
      });


      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl();
      stateProvider.init(cache, mockExecutorService,
            configuration, rpcManager, commandsFactory, cacheNotifier, persistenceManager,
            dataContainer, transactionTable, stateTransferLock, stateConsumer, ef, keyPartitioner, TransactionOriginatorChecker.LOCAL);

      final List<InternalCacheEntry> cacheEntries = new ArrayList<>();
      Object key1 = new TestKey("key1", 0, ch1);
      Object key2 = new TestKey("key2", 0, ch1);
      Object key3 = new TestKey("key3", 1, ch1);
      Object key4 = new TestKey("key4", 1, ch1);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      cacheEntries.add(new ImmortalCacheEntry(key3, "value3"));
      cacheEntries.add(new ImmortalCacheEntry(key4, "value4"));
      when(dataContainer.iterator()).thenAnswer(invocation -> cacheEntries.iterator());
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.emptyList());

      cacheTopology = new CacheTopology(1, 1, ch1, ch1, ch1, CacheTopology.Phase.READ_OLD_WRITE_ALL, ch2.getMembers(), persistentUUIDManager.mapAddresses(ch1.getMembers()));
      stateProvider.onTopologyUpdate(cacheTopology, false);

      log.debug("ch1: " + ch1);
      Set<Integer> segmentsToRequest = ch1.getSegmentsForOwner(members1.get(0));
      List<TransactionInfo> transactions = stateProvider.getTransactionsForSegments(members1.get(0), 1, segmentsToRequest);
      assertEquals(0, transactions.size());

      try {
         stateProvider.getTransactionsForSegments(members1.get(0), 1, SmallIntSet.of(2, numSegments));
         fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
         // expected
      }

      verifyNoMoreInteractions(stateTransferLock);

      stateProvider.startOutboundTransfer(F, 1, Collections.singleton(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      // TestingUtil.sleepThread(15000);
      log.debug("ch2: " + ch2);
      cacheTopology = new CacheTopology(2, 1, ch2, ch2, ch2, CacheTopology.Phase.READ_OLD_WRITE_ALL, ch2.getMembers(), persistentUUIDManager.mapAddresses(ch2.getMembers()));
      stateProvider.onTopologyUpdate(cacheTopology, false);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.startOutboundTransfer(E, 1, Collections.singleton(0), true);

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.stop();

      assertFalse(stateProvider.isStateTransferInProgress());
   }
}
