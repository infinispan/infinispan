package org.infinispan.remoting;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.Response;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;
import org.mockito.Matchers;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractCommandsFactory;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.infinispan.test.fwk.TestCacheManagerFactory.getDefaultCacheConfiguration;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
   private CommandAwareRpcDispatcher commandAwareRpcDispatcher;
   private Address address;
   private RpcDispatcher.Marshaller marshaller;
   private CommandsFactory commandsFactory;
   private ReplicableCommand blockingCacheRpcCommand;
   private ReplicableCommand nonBlockingCacheRpcCommand;
   private ReplicableCommand blockingNonCacheRpcCommand;
   private ReplicableCommand nonBlockingNonCacheRpcCommand;
   private ReplicableCommand blockingSingleRpcCommand;
   private ReplicableCommand nonBlockingSingleRpcCommand;
   private ReplicableCommand blockingMultipleRpcCommand;
   private ReplicableCommand blockingMultipleRpcCommand2;
   private ReplicableCommand nonBlockingMultipleRpcCommand;

   private static ReplicableCommand mockReplicableCommand(boolean blocking) throws Throwable {
      ReplicableCommand mock = mock(ReplicableCommand.class);
      when(mock.canBlock()).thenReturn(blocking);
      doReturn(null).when(mock).perform(Matchers.any(InvocationContext.class));
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
      String cacheName = cache.getName();
      Transport transport = extractGlobalComponent(cacheManager, Transport.class);
      if (transport instanceof JGroupsTransport) {
         commandAwareRpcDispatcher = ((JGroupsTransport) transport).getCommandAwareRpcDispatcher();
         address = ((JGroupsTransport) transport).getChannel().getAddress();
         marshaller = commandAwareRpcDispatcher.getMarshaller();
      } else {
         Assert.fail("Expected a JGroups Transport");
      }
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
      blockingCacheRpcCommand = new ReduceCommand<>("task", null, cacheName, null);
      nonBlockingCacheRpcCommand = new ClusteredGetCommand("key", cacheName, null, false, null, null);
      blockingNonCacheRpcCommand = new CacheTopologyControlCommand(null, CacheTopologyControlCommand.Type.POLICY_GET_STATUS, null, 0);
      //the GetKeyValueCommand is not replicated, but I only need a command that returns false in canBlock()
      nonBlockingNonCacheRpcCommand = new ClusteredGetCommand("key", cacheName, null, false, null, AnyEquivalence.STRING);
      blockingSingleRpcCommand = new SingleRpcCommand(cacheName, blockingReplicableCommand);
      nonBlockingSingleRpcCommand = new SingleRpcCommand(cacheName, nonBlockingReplicableCommand);
      blockingMultipleRpcCommand = new MultipleRpcCommand(Arrays.asList(blockingReplicableCommand, blockingReplicableCommand), cacheName);
      blockingMultipleRpcCommand2 = new MultipleRpcCommand(Arrays.asList(blockingReplicableCommand, nonBlockingReplicableCommand), cacheName);
      nonBlockingMultipleRpcCommand = new MultipleRpcCommand(Arrays.asList(nonBlockingReplicableCommand, nonBlockingReplicableCommand), cacheName);
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
      Assert.assertTrue(blockingMultipleRpcCommand.canBlock());
      Assert.assertTrue(blockingMultipleRpcCommand2.canBlock());

      Assert.assertFalse(nonBlockingCacheRpcCommand.canBlock());
      Assert.assertFalse(nonBlockingNonCacheRpcCommand.canBlock());
      Assert.assertFalse(nonBlockingSingleRpcCommand.canBlock());
      Assert.assertFalse(nonBlockingMultipleRpcCommand.canBlock());
   }

   public void testCacheRpcCommands() throws Exception {
      assertDispatchForCommand(blockingCacheRpcCommand, true);
      assertDispatchForCommand(nonBlockingCacheRpcCommand, false);
   }

   public void testSingleRpcCommand() throws Exception {
      assertDispatchForCommand(blockingSingleRpcCommand, true);
      assertDispatchForCommand(nonBlockingSingleRpcCommand, false);
   }

   public void testMultipleRpcCommand() throws Exception {
      assertDispatchForCommand(blockingMultipleRpcCommand, true);
      assertDispatchForCommand(blockingMultipleRpcCommand2, true);
      assertDispatchForCommand(nonBlockingMultipleRpcCommand, false);
   }

   public void testNonCacheRpcCommands() throws Exception {
      assertDispatchForCommand(blockingNonCacheRpcCommand, true);
      assertDispatchForCommand(nonBlockingNonCacheRpcCommand, false);
   }

   private void assertDispatchForCommand(ReplicableCommand command, boolean expected) throws Exception {
      log.debugf("Testing " + command.getClass().getCanonicalName());
      commandsFactory.initializeReplicableCommand(command, true);
      Message oobRequest = serialize(command, true, address);
      if (oobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      CountDownLatchResponse response = new CountDownLatchResponse();
      commandAwareRpcDispatcher.handle(oobRequest, response);
      response.await(30, TimeUnit.SECONDS);
      Assert.assertEquals(executorService.hasExecutedCommand, expected,
                          "Command " + command.getClass() + " dispatched wrongly.");

      Message nonOobRequest = serialize(command, false, address);
      if (nonOobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      response = new CountDownLatchResponse();
      commandAwareRpcDispatcher.handle(nonOobRequest, response);
      response.await(30, TimeUnit.SECONDS);
      Assert.assertFalse(executorService.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");
   }

   private Message serialize(ReplicableCommand command, boolean oob, Address from) {
      Buffer buffer;
      try {
         buffer = marshaller.objectToBuffer(command);
      } catch (Exception e) {
         //ignore, it will not be replicated
         return null;
      }
      Message message = new Message(null, from, buffer.getBuf(), buffer.getOffset(), buffer.getLength());
      message.setFlag(Message.Flag.NO_TOTAL_ORDER);
      if (oob) {
         message.setFlag(Message.Flag.OOB);
      }
      return message;
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
         return InfinispanCollections.emptyList(); //no-op
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

   private static class CountDownLatchResponse implements Response {

      private final CountDownLatch countDownLatch;

      private CountDownLatchResponse() {
         countDownLatch = new CountDownLatch(1);
      }

      @Override
      public void send(Object reply, boolean is_exception) {
         countDownLatch.countDown();
      }

      @Override
      public void send(Message reply, boolean is_exception) {
         countDownLatch.countDown();
      }

      public boolean await(long time, TimeUnit unit) throws InterruptedException {
         return countDownLatch.await(time, unit);
      }
   }
}
