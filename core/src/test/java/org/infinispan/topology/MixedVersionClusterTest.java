package org.infinispan.topology;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.functional.FunctionalTestUtils.MAX_WAIT_SECS;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.CustomCacheRpcCommand;
import org.infinispan.remoting.rpc.RpcSCI;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.upgrade.UnsupportedException;
import org.infinispan.util.ByteString;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * A test to ensure that an Exception is thrown when a node attempts to send a command to a mixed version cluster where
 * one or more of the cluster members do not support the command.
 */
@Test(testName = "remoting.rpc.MixedVersionClusterTest", groups = "functional")
public class MixedVersionClusterTest extends MultipleCacheManagersTest {

   private static final String TEST_NAME = MixedVersionClusterTest.class.getSimpleName();
   private static final String TEST_CACHE = TEST_NAME;
   private static final int NUM_NODES = 3;

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory(TEST_NAME));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(TEST_NAME));
      IntStream.range(0, NUM_NODES).forEach(i -> addCacheManager(i, globalBuilder(i), NodeVersion.INSTANCE));
      waitForClusterToForm(TEST_CACHE);
   }

   private GlobalConfigurationBuilder globalBuilder(int index) {
      String stateDirectory = tmpDirectory(TEST_NAME, Integer.toString(index));
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.globalState().enable().persistentLocation(stateDirectory).configurationStorage(ConfigurationStorage.OVERLAY);
      return builder;
   }

   private void addCacheManager(int i, GlobalConfigurationBuilder globalBuilder, NodeVersion version) {
      var cm = createClusteredCacheManager(false, globalBuilder, null, new TransportFlags());
      TestingUtil.replaceComponent(cm, Transport.class, new CustomVersionJGroupsTransport(version), true);
      cm.defineConfiguration(TEST_CACHE, cacheConfig());
      cacheManagers.add(i, cm);
      cm.start();
   }

   private Configuration cacheConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC).build();
   }

   public void testUnknownCommand() throws Exception {
      for (int j = 0; j < NUM_NODES; j++) {
         assertFalse(isMixedCluster(j));
         assertEquals(NodeVersion.INSTANCE, getOldestMember(j));
      }

      var newVersion = NodeVersion.from(Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE);
      var cmd = new CustomCacheRpcCommand(ByteString.fromString(TEST_CACHE), "some-value");
      cmd.setSupportedSince(newVersion);

      // Restart nodes in reverse order to simulate a StatefulSet rolling update
      for (int i = NUM_NODES - 1; i > -1; i--) {
         // Stop the node and wait for the rebalance to finish
         var cm = manager(i);
         cm.stop();
         cacheManagers.remove(i);
         waitForClusterToForm(TEST_CACHE);

         // Restart the node with the new NodeVersion and updated SerializationContextInitializer
         var globalBuilder = globalBuilder(i);
         globalBuilder.serialization().addContextInitializer(RpcSCI.INSTANCE);
         addCacheManager(i, globalBuilder, newVersion);
         waitForClusterToForm(TEST_CACHE);

         if (i != 0) {
            // Assert that all nodes are aware we're in a mixed cluster
            for (int j = 0; j < NUM_NODES; j++) {
               assertTrue(isMixedCluster(j));
               assertEquals(NodeVersion.INSTANCE, getOldestMember(j));
            }
            assertCommandFailsOnAllManagers(cmd);
         } else {
            // Assert that all nodes are aware we're no longer in a mixed cluster
            for (int j = 0; j < NUM_NODES; j++) {
               assertFalse(isMixedCluster(j));
               assertEquals(newVersion, getOldestMember(j));
            }
            assertCommandPassesOnAllManagers(cmd);
         }
      }
   }

   private void assertCommandFailsOnAllManagers(CacheRpcCommand cmd) throws Exception {
      for (int i = 0; i < managers().length; i++) assertCommandOnManager(i, cmd, true);
   }

   private void assertCommandPassesOnAllManagers(CacheRpcCommand cmd) throws Exception {
      for (int i = 0; i < managers().length; i++) assertCommandOnManager(i, cmd, false);
   }

   private void assertCommandOnManager(int i, CacheRpcCommand cmd, boolean fail) throws Exception {
      var rpcManager = manager(i).getCache(TEST_CACHE).getAdvancedCache().getRpcManager();
      var syncOpts = rpcManager.getSyncRpcOptions();

      var address = rpcManager.getMembers().stream().filter(a -> !a.equals(rpcManager.getAddress())).findFirst().get();
      assertCommandExec(
            rpcManager.invokeCommand(address, cmd, MapResponseCollector.ignoreLeavers(), syncOpts), fail
      );

      var targets = rpcManager.getMembers();
      assertCommandExec(
            rpcManager.invokeCommand(targets, cmd, MapResponseCollector.ignoreLeavers(), syncOpts), fail
      );

      assertCommandExec(
            rpcManager.invokeCommandOnAll(cmd, MapResponseCollector.ignoreLeavers(), syncOpts), fail
      );

      assertCommandExec(
            rpcManager.invokeCommandStaggered(targets, cmd, MapResponseCollector.ignoreLeavers(), syncOpts), fail
      );

      assertCommandExec(
            rpcManager.invokeCommands(targets, a -> cmd, MapResponseCollector.ignoreLeavers(), syncOpts), fail
      );

      assertCommandExec(() -> rpcManager.sendTo(address, cmd, DeliverOrder.NONE), fail);
      assertCommandExec(() -> rpcManager.sendToMany(targets, cmd, DeliverOrder.NONE), fail);
      assertCommandExec(() -> rpcManager.sendToAll(cmd, DeliverOrder.NONE), fail);
   }

   private void assertCommandExec(Runnable r, boolean expectFail) {
      try {
         r.run();
         if (expectFail) fail();
      } catch (CacheException e) {
         if (!expectFail) fail("Unexpected exception: " + e.getCause());
         assertUnsupportedException(e.getCause());
      }
   }

   private void assertCommandExec(CompletionStage<?> stage, boolean expectFail) throws Exception {
      try {
         stage.toCompletableFuture().get(MAX_WAIT_SECS, TimeUnit.SECONDS);
         if (expectFail) fail("Expected an exception to be thrown on command execution");
      } catch (ExecutionException e) {
         if (!expectFail) fail("Unexpected exception: " + e.getCause());
         assertUnsupportedException(e.getCause());
      }
   }

   private void assertUnsupportedException(Throwable t) {
      assertTrue(t instanceof UnsupportedException);
      assertEquals("Command 'CustomCacheRpcCommand' not yet supported by all cluster members, requires version '127.127.127'", t.getMessage());
   }

   private boolean isMixedCluster(int i) {
      return transport(i).isMixedVersionCluster();
   }

   private NodeVersion getOldestMember(int i) {
      return transport(i).getOldestMember();
   }

   private JGroupsTransport transport(int i) {
      return (JGroupsTransport) TestingUtil.extractGlobalComponent(manager(i), Transport.class);
   }

   public static class CustomVersionJGroupsTransport extends JGroupsTransport {
      public CustomVersionJGroupsTransport(NodeVersion version) {
         super(version);
      }
   }
}
