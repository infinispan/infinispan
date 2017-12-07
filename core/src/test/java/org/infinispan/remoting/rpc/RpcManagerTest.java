package org.infinispan.remoting.rpc;

import static org.infinispan.remoting.responses.SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.2
 */
@Test(groups = "functional", testName = "remoting.rpc.RpcManagerTest")
public class RpcManagerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
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
         rpcManager0.invokeCommands(Arrays.asList(address(0)), a -> command, MapResponseCollector.validOnly(),
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
