package org.infinispan.conflict.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "conflict.resolution.StateReceiverTest")
public class StateReceiverTest extends AbstractInfinispanTest {

   private StateReceiverImpl<Object, Object> stateReceiver;
   private LocalizedCacheTopology localizedCacheTopology;
   private final ExecutorService stateTransferExecutor =
      Executors.newSingleThreadExecutor(getTestThreadFactory("StateTransfer"));

   public void testGetReplicaException() {
      CompletableFuture<Void> taskFuture = new CompletableFuture<>();
      taskFuture.completeExceptionally(new CacheException("Problem encountered retrieving state"));
      initTransferTaskMock(taskFuture);

      CompletableFuture<List<Map<Address, CacheEntry<Object, Object>>>> cf = stateReceiver.getAllReplicasForSegment(0, localizedCacheTopology, 10000);
      Exceptions.expectExecutionException(CacheException.class, cf);
   }

   public void testTopologyChangeDuringSegmentRequest() {
      initTransferTaskMock(new CompletableFuture<>());

      CompletableFuture<List<Map<Address, CacheEntry<Object, Object>>>> cf = stateReceiver.getAllReplicasForSegment(0, localizedCacheTopology, 10000);
      assertTrue(!cf.isCancelled());
      assertTrue(!cf.isCompletedExceptionally());

      // Reduce #nodes to less than numowners to force hash change
      stateReceiver.onDataRehash(createEventImpl(4, 1, Event.Type.DATA_REHASHED));
      assertTrue(cf.isCompletedExceptionally());
      Exceptions.expectExecutionException(CacheException.class, cf);

      stateReceiver.onDataRehash(createEventImpl(4, 4, Event.Type.DATA_REHASHED));
      cf = stateReceiver.getAllReplicasForSegment(1, localizedCacheTopology, 10000);
      assertTrue(!cf.isCompletedExceptionally());
      assertTrue(!cf.isCancelled());
   }

   public void testOldAndInvalidStateIgnored() {
      initTransferTaskMock(new CompletableFuture<>());

      int segmentId = 0;
      stateReceiver.getAllReplicasForSegment(segmentId, localizedCacheTopology, 10000);
      List<Address> sourceAddresses = new ArrayList<>(stateReceiver.getTransferTaskMap(segmentId).keySet());
      Map<Object, Map<Address, CacheEntry<Object, Object>>> receiverKeyMap = stateReceiver.getKeyReplicaMap(segmentId);
      assertEquals(0, receiverKeyMap.size());
      stateReceiver.receiveState(sourceAddresses.get(0), 2, createStateChunks("Key1", "Value1"));
      assertEquals(1, receiverKeyMap.size());
      stateReceiver.receiveState(new TestAddress(5), 2, createStateChunks("Key2", "Value2"));
      assertEquals(1, receiverKeyMap.size());
      stateReceiver.receiveState(sourceAddresses.get(1), 1, new ArrayList<>());
      assertEquals(1, receiverKeyMap.size());
   }

   @Test(expectedExceptions = CancellationException.class)
   public void testRequestCanBeCancelledDuringTransfer() throws Exception {
      // Init transfer that blocks and call stop() so the future should complete with CancellationException
      InboundTransferTask task = mock(InboundTransferTask.class);
      when(task.requestSegments()).thenAnswer(invocationOnMock -> {
         TestingUtil.sleepThread(1000);
         return CompletableFuture.completedFuture(new HashMap<>());
      });
      doReturn(task).when(stateReceiver).createTransferTask(any(Integer.class), any(Address.class), any(CacheTopology.class), any(Long.class));

      CompletableFuture<List<Map<Address, CacheEntry<Object, Object>>>> future = stateReceiver.getAllReplicasForSegment(0, localizedCacheTopology, 10000);
      future.whenComplete((result, throwable) -> {
         assertNull(result);
         assertNotNull(throwable);
         assertTrue(throwable instanceof CancellationException);
      });
      stateReceiver.stop();
      future.get();
   }

   @BeforeMethod
   private void createAndInitStateReceiver() {
      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      InternalDataContainer<?, ?> dataContainer = mock(InternalDataContainer.class);
      RpcManager rpcManager = mock(RpcManager.class);
      CacheNotifier<?, ?> cacheNotifier = mock(CacheNotifier.class);

      Answer<?> answer = invocation -> {
         Collection<Address> recipients = (Collection<Address>) invocation.getArguments()[0];
         Address recipient = recipients.iterator().next();
         Map<Address, Response> results = new HashMap<>(1);
         results.put(recipient, SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
         return results;
      };
      when(rpcManager.invokeCommand(any(Collection.class), any(ConflictResolutionStartCommand.class), any(), any())).thenAnswer(answer);
      when(rpcManager.invokeCommand(any(Collection.class), any(StateTransferCancelCommand.class), any(), any())).thenAnswer(answer);

      when(rpcManager.getSyncRpcOptions()).thenAnswer(invocation -> new RpcOptions(DeliverOrder.PER_SENDER, 10000, TimeUnit.MILLISECONDS));

      StateReceiverImpl<Object, Object> stateReceiver = new StateReceiverImpl<>();
      TestingUtil.inject(stateReceiver, cacheNotifier, commandsFactory, dataContainer, rpcManager, stateTransferExecutor);
      stateReceiver.start();
      stateReceiver.onDataRehash(createEventImpl(2, 4, Event.Type.DATA_REHASHED));
      this.localizedCacheTopology = createLocalizedCacheTopology(4);
      this.stateReceiver = spy(stateReceiver);
   }

   @AfterClass(alwaysRun = true)
   public void stopExecutor() {
      stateTransferExecutor.shutdownNow();
   }

   private void initTransferTaskMock(CompletableFuture<Void> completableFuture) {
      InboundTransferTask task = mock(InboundTransferTask.class);
      when(task.requestSegments()).thenReturn(completableFuture);
      doReturn(task).when(stateReceiver).createTransferTask(any(Integer.class), any(Address.class), any(CacheTopology.class), any(Long.class));
   }

   private Collection<StateChunk> createStateChunks(Object key, Object value) {
      Collection<InternalCacheEntry<?, ?>> entries = Collections.singleton(new ImmortalCacheEntry(key, value));
      return Collections.singleton(new StateChunk(0, entries, true));
   }

   private ConsistentHash createConsistentHash(int numberOfNodes) {
      PersistentUUIDManager persistentUUIDManager = new PersistentUUIDManagerImpl();
      List<Address> addresses = new ArrayList<>(numberOfNodes);
      for (int i = 0; i < numberOfNodes; i++) {
         Address address = new TestAddress(i);
         addresses.add(address);
         persistentUUIDManager.addPersistentAddressMapping(address, PersistentUUID.randomUUID());
      }

      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      return chf.create(2, 40, addresses, null);
   }

   private LocalizedCacheTopology createLocalizedCacheTopology(int numberOfNodes) {
      ConsistentHash hash = createConsistentHash(numberOfNodes);
      CacheTopology topology = new CacheTopology(-1,  -1, hash, null, CacheTopology.Phase.NO_REBALANCE, hash.getMembers(), null);
      return new LocalizedCacheTopology(CacheMode.DIST_SYNC, topology, new HashFunctionPartitioner(), hash.getMembers().get(0), true);
   }

   private EventImpl createEventImpl(int topologyId, int numberOfNodes, Event.Type type) {
      EventImpl event = EventImpl.createEvent(null, type);
      ConsistentHash hash = createConsistentHash(numberOfNodes);
      event.setReadConsistentHashAtEnd(hash);
      event.setWriteConsistentHashAtEnd(hash);
      event.setNewTopologyId(topologyId);
      event.setPre(true);
      return event;
   }
}
