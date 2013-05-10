/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.remoting;

import org.infinispan.Cache;
import org.infinispan.EmbeddedMetadata;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.util.InfinispanCollections;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.extractCommandsFactory;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;

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

   @BeforeClass
   public void setUp() {
      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      DummyExecutorFactory factory = new DummyExecutorFactory();

      globalConfigurationBuilder.remoteCommandsExecutor().factory(factory);
      configurationBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);

      cacheManager = createClusteredCacheManager(globalConfigurationBuilder, configurationBuilder);
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
      commandsFactory = extractCommandsFactory(cache);
      executorService = factory.getExecutorService();

      GetKeyValueCommand getKeyValueCommand =
            new GetKeyValueCommand("key", InfinispanCollections.<Flag>emptySet(), false);
      PutKeyValueCommand putKeyValueCommand =
            new PutKeyValueCommand("key", "value", false, null,
                  new EmbeddedMetadata.Builder().build(), InfinispanCollections.<Flag>emptySet());

      //populate commands
      blockingCacheRpcCommand = new ReduceCommand<Object, Object>(cacheName);
      nonBlockingCacheRpcCommand = new ClusteredGetCommand(cacheName);
      blockingNonCacheRpcCommand = new CacheTopologyControlCommand();
      //the GetKeyValueCommand is not replicated, but I only need a command that returns false in canBlock()
      nonBlockingNonCacheRpcCommand = new GetKeyValueCommand("key", InfinispanCollections.<Flag>emptySet(), false);
      blockingSingleRpcCommand = new SingleRpcCommand(cacheName, putKeyValueCommand);
      nonBlockingSingleRpcCommand = new SingleRpcCommand(cacheName, getKeyValueCommand);
      blockingMultipleRpcCommand = new MultipleRpcCommand(Arrays.<ReplicableCommand>asList(putKeyValueCommand, putKeyValueCommand), cacheName);
      blockingMultipleRpcCommand2 = new MultipleRpcCommand(Arrays.<ReplicableCommand>asList(putKeyValueCommand, getKeyValueCommand), cacheName);
      nonBlockingMultipleRpcCommand = new MultipleRpcCommand(Arrays.<ReplicableCommand>asList(getKeyValueCommand, getKeyValueCommand), cacheName);
   }

   @AfterClass
   public void tearDown() {
      if (cacheManager != null) {
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
      commandAwareRpcDispatcher.handle(oobRequest, null);
      Assert.assertEquals(executorService.hasExecutedCommand, expected,
                          "Command " + command.getClass() + " dispatched wrongly.");

      Message nonOobRequest = serialize(command, false, address);
      if (nonOobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(nonOobRequest, null);
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
      if (oob) {
         message.setFlag(Message.Flag.OOB);
      }
      return message;
   }

   private class DummyExecutorFactory implements ExecutorFactory {

      private final DummyTaskCountExecutorService executorService;

      private DummyExecutorFactory() {
         executorService = new DummyTaskCountExecutorService();
      }

      @Override
      public ExecutorService getExecutor(Properties p) {
         return executorService;
      }

      public DummyTaskCountExecutorService getExecutorService() {
         return executorService;
      }
   }

   private class DummyTaskCountExecutorService extends AbstractExecutorService {

      private volatile boolean hasExecutedCommand;

      @Override
      public void execute(Runnable command) {
         hasExecutedCommand = true;
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
}
