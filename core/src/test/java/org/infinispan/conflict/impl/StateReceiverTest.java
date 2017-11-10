package org.infinispan.conflict.impl;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.topology.PersistentUUIDManagerImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "conflict.resolution.StateReceiverTest")
public class StateReceiverTest extends AbstractInfinispanTest {

   private StateReceiverImpl<Object, Object> stateReceiver;
   private LocalizedCacheTopology localizedCacheTopology;

   public void testGetReplicaException() throws Exception {
      CompletableFuture<Void> taskFuture = new CompletableFuture<>();
      taskFuture.completeExceptionally(new CacheException("Problem encountered retrieving state"));
      initTransferTaskMock(taskFuture);

      CompletableFuture<List<Map<Address, CacheEntry<Object, Object>>>> cf = stateReceiver.getAllReplicasForSegment(0, localizedCacheTopology);
      Exceptions.expectExecutionException(CacheException.class, cf);
   }

   public void testTopologyChangeDuringSegmentRequest() throws Exception {
      initTransferTaskMock(new CompletableFuture<>());

      CompletableFuture<List<Map<Address, CacheEntry<Object, Object>>>> cf = stateReceiver.getAllReplicasForSegment(0, localizedCacheTopology);
      assertTrue(!cf.isCancelled());
      assertTrue(!cf.isCompletedExceptionally());

      // Reduce #nodes to less than numowners to force hash change
      stateReceiver.onDataRehash(createEventImpl(4, 1, Event.Type.DATA_REHASHED));
      assertTrue(cf.isCompletedExceptionally());
      Exceptions.expectExecutionException(CacheException.class, cf);

      stateReceiver.onDataRehash(createEventImpl(4, 4, Event.Type.DATA_REHASHED));
      cf = stateReceiver.getAllReplicasForSegment(1, localizedCacheTopology);
      assertTrue(!cf.isCompletedExceptionally());
      assertTrue(!cf.isCancelled());
   }

   public void testOldAndInvalidStateIgnored() {
      initTransferTaskMock(new CompletableFuture<>());

      int segmentId = 0;
      stateReceiver.getAllReplicasForSegment(segmentId, localizedCacheTopology);
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

   @BeforeMethod
   private void createAndInitStateReceiver() {
      Cache cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      DataContainer dataContainer = mock(DataContainer.class);
      RpcManager rpcManager = mock(RpcManager.class);

      when(rpcManager.invokeRemotely(any(Collection.class), any(StateRequestCommand.class), any(RpcOptions.class)))
            .thenAnswer(invocation -> {
               Collection<Address> recipients = (Collection<Address>) invocation.getArguments()[0];
               Address recipient = recipients.iterator().next();
               StateRequestCommand cmd = (StateRequestCommand) invocation.getArguments()[1];
               Map<Address, Response> results = new HashMap<>(1);
               if (cmd.getType().equals(StateRequestCommand.Type.START_CONSISTENCY_CHECK)
                     || cmd.getType().equals(StateRequestCommand.Type.CANCEL_CONSISTENCY_CHECK)) {
                  results.put(recipient, SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
               }
               return results;
            });

      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class))).thenAnswer(invocation -> {
         Object[] args = invocation.getArguments();
         return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], DeliverOrder.PER_SENDER);
      });
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().clustering().cacheMode(CacheMode.DIST_SYNC);
      when(cache.getCacheConfiguration()).thenReturn(cb.build());

      StateReceiverImpl stateReceiver = new StateReceiverImpl();
      TestingUtil.inject(stateReceiver, cache, commandsFactory, dataContainer, rpcManager);
      stateReceiver.onDataRehash(createEventImpl(2, 4, Event.Type.DATA_REHASHED));
      this.localizedCacheTopology = createLocalizedCacheTopology(4);
      this.stateReceiver = spy(stateReceiver);
   }

   private void initTransferTaskMock(CompletableFuture<Void> completableFuture) {
      InboundTransferTask task = mock(InboundTransferTask.class);
      when(task.requestSegments()).thenReturn(completableFuture);
      doReturn(task).when(stateReceiver).createTransferTask(any(Integer.class), any(Address.class), any(CacheTopology.class));
   }

   private Collection<StateChunk> createStateChunks(Object key, Object value) {
      Collection<InternalCacheEntry> entries = Collections.singleton(new ImmortalCacheEntry(key, value));
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
      return chf.create(MurmurHash3.getInstance(), 2, 40, addresses, null);
   }

   private LocalizedCacheTopology createLocalizedCacheTopology(int numberOfNodes) {
      ConsistentHash hash = createConsistentHash(numberOfNodes);
      CacheTopology topology = new CacheTopology(-1,  -1, hash, null, CacheTopology.Phase.NO_REBALANCE, hash.getMembers(), null);
      return new LocalizedCacheTopology(CacheMode.DIST_SYNC, topology, new HashFunctionPartitioner(), hash.getMembers().get(0));
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
