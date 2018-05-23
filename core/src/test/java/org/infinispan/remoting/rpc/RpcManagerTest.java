package org.infinispan.remoting.rpc;

import static org.infinispan.remoting.responses.SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ReplicatedConsistentHashFactory;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.jgroups.util.NameCache;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.2
 */
@Test(groups = "functional", testName = "remoting.rpc.RpcManagerTest")
public class RpcManagerTest extends MultipleCacheManagersTest {
   private static final JGroupsAddress SUSPECT = new JGroupsAddress(UUID.randomUUID());

   @Override
   protected void createCacheManagers() throws Throwable {
      NameCache.add(SUSPECT.getJGroupsAddress(), "SUSPECT");

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      createCluster(builder, 3);
      waitForClusterToForm();
   }

   public void testInvokeCommand1() throws Exception {
      ClusteredGetCommand command =
            TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

      Exceptions.expectException(IllegalArgumentException.class, () ->
         rpcManager0.invokeCommand(address(0), command, SingleResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions()));

      command.setTopologyId(rpcManager0.getTopologyId());
      CompletionStage<ValidResponse> stage1 =
         rpcManager0.invokeCommand(address(0), command, SingleResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      assertResponse(null, stage1);

      CompletionStage<ValidResponse> stage2 =
         rpcManager0.invokeCommand(address(1), command, SingleResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      assertResponse(SUCCESSFUL_EMPTY_RESPONSE, stage2);

      CompletionStage<ValidResponse> stage3 =
         rpcManager0.invokeCommand(SUSPECT, command, SingleResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      Exceptions.expectExecutionException(SuspectException.class, stage3.toCompletableFuture());
   }

   public void testInvokeCommandCollection() throws Exception {
      ClusteredGetCommand command =
         TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

      Exceptions.expectException(IllegalArgumentException.class, () ->
         rpcManager0.invokeCommand(Arrays.asList(address(0)), command, SingleResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions()));

      command.setTopologyId(rpcManager0.getTopologyId());
      CompletionStage<Map<Address, Response>> stage1 =
         rpcManager0.invokeCommand(Arrays.asList(address(0)), command, MapResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      assertResponse(Collections.emptyMap(), stage1);

      CompletionStage<Map<Address, Response>> stage2 =
         rpcManager0.invokeCommand(Arrays.asList(address(1)), command, MapResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      assertResponse(Collections.singletonMap(address(1), SUCCESSFUL_EMPTY_RESPONSE), stage2);

      CompletionStage<Map<Address, Response>> stage3 =
         rpcManager0.invokeCommand(Arrays.asList(address(0), address(1)), command,
                                   MapResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
      assertResponse(Collections.singletonMap(address(1), SUCCESSFUL_EMPTY_RESPONSE), stage3);
   }

   public void testInvokeCommandCollectionSuspect() throws Exception {
      ClusteredGetCommand command =
         TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

      command.setTopologyId(rpcManager0.getTopologyId());
      CompletionStage<Map<Address, Response>> stage1 =
         rpcManager0.invokeCommand(Arrays.asList(SUSPECT), command, MapResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      Exceptions.expectExecutionException(SuspectException.class, stage1.toCompletableFuture());

      CompletionStage<Map<Address, Response>> stage2 =
         rpcManager0.invokeCommand(Arrays.asList(address(0), SUSPECT), command, MapResponseCollector.validOnly(),
                                   rpcManager0.getSyncRpcOptions());
      Exceptions.expectExecutionException(SuspectException.class, stage2.toCompletableFuture());

      CompletionStage<Map<Address, Response>> stage3 =
         rpcManager0.invokeCommand(Arrays.asList(address(0), address(1), SUSPECT), command,
                                   MapResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
      Exceptions.expectExecutionException(SuspectException.class, stage3.toCompletableFuture());
   }

   public void testInvokeCommandOnAll() throws Exception {
      ClusteredGetCommand command =
         TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

      Exceptions.expectException(IllegalArgumentException.class, () ->
         rpcManager0.invokeCommandOnAll(command, SingleResponseCollector.validOnly(),
                                        rpcManager0.getSyncRpcOptions()));

      command.setTopologyId(rpcManager0.getTopologyId());
      CompletionStage<Map<Address, Response>> stage1 =
         rpcManager0.invokeCommandOnAll(command, MapResponseCollector.validOnly(),
                                        rpcManager0.getSyncRpcOptions());
      assertResponse(makeMap(address(1), SUCCESSFUL_EMPTY_RESPONSE, address(2), SUCCESSFUL_EMPTY_RESPONSE), stage1);
   }

   public void testInvokeCommandOnAllSuspect() throws Exception {
      DistributionManager distributionManager = cache(0).getAdvancedCache().getDistributionManager();
      CacheTopology initialTopology = distributionManager.getCacheTopology();
      assertEquals(CacheTopology.Phase.NO_REBALANCE, initialTopology.getPhase());

      try {
         ClusteredGetCommand command =
            TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
         RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

         // Add a node to the cache topology, but not to the JGroups cluster view
         List<Address> newMembers = new ArrayList<>(initialTopology.getMembers());
         newMembers.add(SUSPECT);
         ConsistentHash newCH = new ReplicatedConsistentHashFactory().create(MurmurHash3.getInstance(), 1, 1,
                                                                             newMembers, null);
         CacheTopology suspectTopology =
            new CacheTopology(initialTopology.getTopologyId(), initialTopology.getRebalanceId(), newCH, null, null,
                              CacheTopology.Phase.NO_REBALANCE, newCH.getMembers(), null);
         distributionManager.setCacheTopology(suspectTopology);

         command.setTopologyId(rpcManager0.getTopologyId());
         CompletionStage<Map<Address, Response>> stage1 =
            rpcManager0.invokeCommandOnAll(command, MapResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
         Exceptions.expectExecutionException(SuspectException.class, stage1.toCompletableFuture());
      } finally {
         distributionManager.setCacheTopology(initialTopology);
      }
   }

   public void testInvokeCommandStaggered() throws Exception {
      ClusteredGetCommand command =
         TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

      Exceptions.expectException(IllegalArgumentException.class, () ->
         rpcManager0.invokeCommandStaggered(Arrays.asList(address(0)), command, SingleResponseCollector.validOnly(),
                                            rpcManager0.getSyncRpcOptions()));

      command.setTopologyId(rpcManager0.getTopologyId());
      CompletionStage<ValidResponse> stage1 =
         rpcManager0.invokeCommandStaggered(Arrays.asList(address(0)), command, SingleResponseCollector.validOnly(),
                                            rpcManager0.getSyncRpcOptions());
      assertResponse(null, stage1);

      CompletionStage<ValidResponse> stage2 =
         rpcManager0.invokeCommandStaggered(Arrays.asList(address(1)), command, SingleResponseCollector.validOnly(),
                                            rpcManager0.getSyncRpcOptions());
      assertResponse(SUCCESSFUL_EMPTY_RESPONSE, stage2);

      CompletionStage<ValidResponse> stage3 =
         rpcManager0.invokeCommandStaggered(Arrays.asList(address(0), address(1)), command,
                                            SingleResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
      assertResponse(SUCCESSFUL_EMPTY_RESPONSE, stage3);

      CompletionStage<ValidResponse> stage4 =
         rpcManager0.invokeCommandStaggered(Arrays.asList(address(0), address(1), address(2)), command,
                                            SingleResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
      assertResponse(SUCCESSFUL_EMPTY_RESPONSE, stage4);
   }

   public void testInvokeCommands() throws Exception {
      ClusteredGetCommand command =
         TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager0 = cache(0).getAdvancedCache().getRpcManager();

      Exceptions.expectException(IllegalArgumentException.class, () -> {
         rpcManager0.invokeCommands(Arrays.asList(address(1)), a -> command, MapResponseCollector.validOnly(),
                                    rpcManager0.getSyncRpcOptions());
      });

      command.setTopologyId(rpcManager0.getTopologyId());
      CompletionStage<Map<Address, Response>> stage1 =
         rpcManager0.invokeCommands(Arrays.asList(address(0)), a -> command, MapResponseCollector.validOnly(),
                                    rpcManager0.getSyncRpcOptions());
      assertResponse(Collections.emptyMap(), stage1);

      CompletionStage<Map<Address, Response>> stage2 =
         rpcManager0.invokeCommands(Arrays.asList(address(1)), a -> command, MapResponseCollector.validOnly(),
                                    rpcManager0.getSyncRpcOptions());
      assertResponse(Collections.singletonMap(address(1), SUCCESSFUL_EMPTY_RESPONSE), stage2);

      CompletionStage<Map<Address, Response>> stage3 =
         rpcManager0.invokeCommands(Arrays.asList(address(0), address(1)), a -> command,
                                    MapResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
      assertResponse(Collections.singletonMap(address(1), SUCCESSFUL_EMPTY_RESPONSE), stage3);

      CompletionStage<Map<Address, Response>> stage4 =
         rpcManager0.invokeCommands(Arrays.asList(address(0), address(1), address(2)), a -> command,
                                    MapResponseCollector.validOnly(), rpcManager0.getSyncRpcOptions());
      assertResponse(makeMap(address(1), SUCCESSFUL_EMPTY_RESPONSE, address(2), SUCCESSFUL_EMPTY_RESPONSE), stage4);
   }

   private <T> void assertResponse(T expected, CompletionStage<T> stage2) {
      assertEquals(expected, stage2.toCompletableFuture().join());
   }

   private <T, U> Map<T, U> makeMap(T a1, U r1, T a2, U r2) {
      Map<T, U> map = new HashMap<>();
      map.put(a1, r1);
      map.put(a2, r2);
      return map;
   }
}
