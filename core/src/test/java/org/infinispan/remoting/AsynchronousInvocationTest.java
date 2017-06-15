package org.infinispan.remoting;

import static org.infinispan.test.TestingUtil.extractCommandsFactory;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.getDefaultCacheConfiguration;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests the Asynchronous Invocation API and checks if the commands are correctly processed (or JGroups or Infinispan
 * thread pool)
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "remoting.AsynchronousInvocationTest")
public class AsynchronousInvocationTest extends AbstractInfinispanTest {

   private EmbeddedCacheManager cacheManager;
   private DummyTaskCountExecutorService executorService;
   private InboundInvocationHandler invocationHandler;
   private Address address;
   private CommandsFactory commandsFactory;
   private ReplicableCommand blockingCacheRpcCommand;
   private ReplicableCommand nonBlockingCacheRpcCommand;
   private ReplicableCommand blockingNonCacheRpcCommand;
   private ReplicableCommand nonBlockingNonCacheRpcCommand;
   private ReplicableCommand blockingSingleRpcCommand;
   private ReplicableCommand nonBlockingSingleRpcCommand;

   private static ReplicableCommand mockReplicableCommand(boolean blocking) throws Throwable {
      ReplicableCommand mock = mock(ReplicableCommand.class);
      when(mock.canBlock()).thenReturn(blocking);
      doReturn(null).when(mock).invokeAsync();
      return mock;
   }

   @BeforeClass
   public void setUp() throws Throwable {
      executorService = new DummyTaskCountExecutorService();
      final BlockingTaskAwareExecutorService remoteExecutorService = new BlockingTaskAwareExecutorServiceImpl("AsynchronousInvocationTest-Controller", executorService,
                                                                                                              TIME_SERVICE);
      ConfigurationBuilder builder = getDefaultCacheConfiguration(false);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      cacheManager = createClusteredCacheManager(builder);
      Cache<Object, Object> cache = cacheManager.getCache();
      ByteString cacheName = ByteString.fromString(cache.getName());
      Transport transport = extractGlobalComponent(cacheManager, Transport.class);
      address = transport.getAddress();
      invocationHandler = TestingUtil.extractGlobalComponent(cacheManager, InboundInvocationHandler.class);
      ComponentRegistry registry = cache.getAdvancedCache().getComponentRegistry();
      registry.registerComponent(remoteExecutorService, KnownComponentNames.REMOTE_COMMAND_EXECUTOR);
      registry.rewire();
      GlobalComponentRegistry globalRegistry = cache.getCacheManager().getGlobalComponentRegistry();
      globalRegistry.registerComponent(remoteExecutorService, KnownComponentNames.REMOTE_COMMAND_EXECUTOR);
      globalRegistry.rewire();

      commandsFactory = extractCommandsFactory(cache);

      ReplicableCommand nonBlockingReplicableCommand = mockReplicableCommand(false);
      ReplicableCommand blockingReplicableCommand = mockReplicableCommand(true);

      //populate commands
      blockingCacheRpcCommand = new StreamRequestCommand<>(cacheName);
      nonBlockingCacheRpcCommand = new ClusteredGetCommand("key", cacheName, EnumUtil.EMPTY_BIT_SET);
      blockingNonCacheRpcCommand = new CacheTopologyControlCommand(null, CacheTopologyControlCommand.Type.POLICY_GET_STATUS, null, 0);
      //the GetKeyValueCommand is not replicated, but I only need a command that returns false in canBlock()
      nonBlockingNonCacheRpcCommand = new ClusteredGetCommand("key", cacheName, EnumUtil.EMPTY_BIT_SET);
      blockingSingleRpcCommand = new SingleRpcCommand(cacheName, blockingReplicableCommand);
      nonBlockingSingleRpcCommand = new SingleRpcCommand(cacheName, nonBlockingReplicableCommand);
   }

   @AfterClass
   public void tearDown() {
      if (cacheManager != null) {
         cacheManager.getGlobalComponentRegistry().getComponent(ExecutorService.class, KnownComponentNames.REMOTE_COMMAND_EXECUTOR).shutdownNow();
         cacheManager.stop();
      }
   }

   public void testCommands() {
      //if some of these tests fails, we need to pick another command to make the assertions true
      Assert.assertTrue(blockingCacheRpcCommand.canBlock());
      Assert.assertTrue(blockingNonCacheRpcCommand.canBlock());
      Assert.assertTrue(blockingSingleRpcCommand.canBlock());

      Assert.assertFalse(nonBlockingCacheRpcCommand.canBlock());
      Assert.assertFalse(nonBlockingNonCacheRpcCommand.canBlock());
      Assert.assertFalse(nonBlockingSingleRpcCommand.canBlock());
   }

   public void testCacheRpcCommands() throws Exception {
      assertDispatchForCommand(blockingCacheRpcCommand, true);
      assertDispatchForCommand(nonBlockingCacheRpcCommand, false);
   }

   public void testSingleRpcCommand() throws Exception {
      assertDispatchForCommand(blockingSingleRpcCommand, true);
      assertDispatchForCommand(nonBlockingSingleRpcCommand, false);
   }

   public void testNonCacheRpcCommands() throws Exception {
      assertDispatchForCommand(blockingNonCacheRpcCommand, true);
      assertDispatchForCommand(nonBlockingNonCacheRpcCommand, false);
   }

   private void assertDispatchForCommand(ReplicableCommand command, boolean expected) throws Exception {
      log.debugf("Testing " + command.getClass().getCanonicalName());
      commandsFactory.initializeReplicableCommand(command, true);
      executorService.reset();
      CountDownLatchResponse response = new CountDownLatchResponse();
      invocationHandler.handleFromCluster(address, command, response, DeliverOrder.NONE);
      response.await(30, TimeUnit.SECONDS);
      Assert.assertEquals(executorService.hasExecutedCommand, expected,
                          "Command " + command.getClass() + " dispatched wrongly.");

      executorService.reset();
      response = new CountDownLatchResponse();
      invocationHandler.handleFromCluster(address, command, response, DeliverOrder.PER_SENDER);
      response.await(30, TimeUnit.SECONDS);
      Assert.assertFalse(executorService.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");
   }

   private class DummyTaskCountExecutorService extends AbstractExecutorService {

      private volatile boolean hasExecutedCommand;

      @Override
      public void execute(Runnable command) {
         hasExecutedCommand = true;
         command.run();
      }

      public void reset() {
         hasExecutedCommand = false;
      }

      @Override
      public void shutdown() {
         //no-op
      }

      @Override
      public List<Runnable> shutdownNow() {
         return Collections.emptyList(); //no-op
      }

      @Override
      public boolean isShutdown() {
         return false; //no-op
      }

      @Override
      public boolean isTerminated() {
         return false; //no-op
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         return false; //no-op
      }
   }

   private static class CountDownLatchResponse implements Reply {

      private final CountDownLatch countDownLatch;

      private CountDownLatchResponse() {
         countDownLatch = new CountDownLatch(1);
      }

      public boolean await(long time, TimeUnit unit) throws InterruptedException {
         return countDownLatch.await(time, unit);
      }

      @Override
      public void reply(Response response) {
         countDownLatch.countDown();
      }
   }
}
