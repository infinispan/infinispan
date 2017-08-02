package org.infinispan.remoting.rpc;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @since 9.1
 */
public class RpcManagerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      createCluster(builder, 3);
      waitForClusterToForm();
   }

   @Test
   public void testInvokeCommand1() throws Exception {
      ClusteredGetCommand clusteredGetCommand =
            TestingUtil.extractCommandsFactory(cache(0)).buildClusteredGetCommand("key", 0L);
      RpcManager rpcManager = cache(0).getAdvancedCache().getRpcManager();
      Exceptions.expectException(IllegalArgumentException.class, () ->
            rpcManager.invokeCommand(address(0), clusteredGetCommand, SingleResponseCollector.INSTANCE,
                                     rpcManager.getSyncRpcOptions()));

      clusteredGetCommand.setTopologyId(rpcManager.getTopologyId());
      CompletionStage<Response> stage1 =
            rpcManager.invokeCommand(address(0), clusteredGetCommand, SingleResponseCollector.INSTANCE,
                                     rpcManager.getSyncRpcOptions());
      assertEquals(null, stage1.toCompletableFuture().join());

      CompletionStage<Response> stage2 =
            rpcManager.invokeCommand(address(1), clusteredGetCommand, SingleResponseCollector.INSTANCE,
                                     rpcManager.getSyncRpcOptions());
      assertEquals(SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE, stage2.toCompletableFuture().join());
   }

   @Test
   public void testInvokeCommandCollection() throws Exception {
   }

   @Test
   public void testInvokeCommandOnAll() throws Exception {
   }

   @Test
   public void testInvokeCommandStaggered() throws Exception {
   }

   @Test
   public void testInvokeCommands() throws Exception {
   }

   @Test
   public void testInvokeRemotelyAsync() throws Exception {
   }

   @Test
   public void testInvokeRemotely() throws Exception {
   }

   @Test
   public void testInvokeRemotely1() throws Exception {
   }

   @Test
   public void testSendTo() throws Exception {
   }

   @Test
   public void testSendToMany() throws Exception {
   }

   @Test
   public void testSendToAll() throws Exception {
   }
}
