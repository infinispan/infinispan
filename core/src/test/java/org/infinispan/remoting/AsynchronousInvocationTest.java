/*
 * JBoss, Home of Professional Open Source                           
 *  Copyright 2013 Red Hat Inc. and/or its affiliates and other       
 *  contributors as indicated by the @author tags. All rights reserved
 *  See the copyright.txt in the distribution for a full listing of   
 *  individual contributors.                                          
 *                                                                    
 *  This is free software; you can redistribute it and/or modify it   
 *  under the terms of the GNU Lesser General Public License as       
 *  published by the Free Software Foundation; either version 2.1 of  
 *  the License, or (at your option) any later version.               
 *                                                                    
 *  This software is distributed in the hope that it will be useful,  
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of    
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU  
 *  Lesser General Public License for more details.                   
 *                                                                    
 *  You should have received a copy of the GNU Lesser General Public  
 *  License along with this software; if not, write to the Free       
 *  Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 *  02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.remoting;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.ClassFinder;
import org.infinispan.util.InfinispanCollections;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Buffer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.test.TestingUtil.*;
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
   private String cacheName;
   private DummyTaskCountExecutorService executorService;
   private CommandAwareRpcDispatcher commandAwareRpcDispatcher;
   private Address address;
   private RpcDispatcher.Marshaller marshaller;
   private TransactionFactory transactionFactory;
   private CommandsFactory commandsFactory;
   @BeforeMethod(alwaysRun = true)
   public void setUp() {
      GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      DummyExecutorFactory factory = new DummyExecutorFactory();

      globalConfigurationBuilder.remoteCommandsExecutor().factory(factory);
      configurationBuilder.clustering().cacheMode(CacheMode.DIST_SYNC);

      cacheManager = createClusteredCacheManager(globalConfigurationBuilder, configurationBuilder);
      Cache<Object, Object> cache = cacheManager.getCache();
      cacheName = cache.getName();
      Transport transport = extractGlobalComponent(cacheManager, Transport.class);
      if (transport instanceof JGroupsTransport) {
         commandAwareRpcDispatcher = ((JGroupsTransport) transport).getCommandAwareRpcDispatcher();
         address = ((JGroupsTransport) transport).getChannel().getAddress();
         marshaller = commandAwareRpcDispatcher.getMarshaller();
      } else {
         Assert.fail("Expected a JGroups Transport");
      }
      transactionFactory = extractComponent(cache, TransactionFactory.class);
      commandsFactory = extractCommandsFactory(cache);
      executorService = factory.getExecutorService();
   }

   @AfterMethod
   public void tearDown() {
      if (cacheManager != null) {
         cacheManager.stop();
      }
   }

   public void testCacheRpcCommands() throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(CacheRpcCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers()) || LocalCommand.class.isAssignableFrom(clazz)) {
            continue;
         }

         if (RemoveCacheCommand.class.isAssignableFrom(clazz) || SingleRpcCommand.class.isAssignableFrom(clazz) ||
               MultipleRpcCommand.class.isAssignableFrom(clazz) || AbstractTransactionBoundaryCommand.class.isAssignableFrom(clazz)) {
            //special cases
            continue;
         }

         CacheRpcCommand command = (CacheRpcCommand) clazz.getConstructor(String.class).newInstance(cacheName);
         assertDispatchForCommand(command);
      }
   }
   
   public void testCommitCommand() throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(CommitCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            continue;
         } 

         CacheRpcCommand command = (CacheRpcCommand) clazz.getConstructor(String.class, GlobalTransaction.class)
               .newInstance(cacheName, transactionFactory.newGlobalTransaction());
         assertDispatchForCommand(command);
      }
   }

   public void testRollbackCommand() throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(RollbackCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            continue;
         }

         CacheRpcCommand command = (CacheRpcCommand) clazz.getConstructor(String.class, GlobalTransaction.class)
               .newInstance(cacheName, transactionFactory.newGlobalTransaction());
         assertDispatchForCommand(command);
      }
   }

   public void testPrepareCommand() throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(PrepareCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            continue;
         }

         CacheRpcCommand command = (CacheRpcCommand) clazz.getConstructor(String.class, GlobalTransaction.class, List.class, boolean.class)
               .newInstance(cacheName, transactionFactory.newGlobalTransaction(), InfinispanCollections.emptyList(), true);
         assertDispatchForCommand(command);
      }
   }
   
   public void testLockControlCommand() throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(LockControlCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            continue;
         }

         CacheRpcCommand command = (CacheRpcCommand) clazz.getConstructor(Collection.class, String.class, Set.class, GlobalTransaction.class)
               .newInstance(InfinispanCollections.emptyList(), cacheName, InfinispanCollections.emptySet(), transactionFactory.newGlobalTransaction());
         assertDispatchForCommand(command);
      }      
   }

   public void testRemoveCacheCommandInOOB() throws Exception {
      CacheRpcCommand command = new RemoveCacheCommand(cacheName, null, null, null);

      log.debugf("Testing " + command.getClass().getCanonicalName());
      Message oobRequest = serialize(command, true);
      if (oobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(oobRequest, null);
      Assert.assertEquals(executorService.hasExecutedCommand, command.canBlock(),
                          "Command " + command.getClass() + " dispatched wrongly.");
   }

   public void testRemoveCacheCommandInNonOOB() throws Exception {
      CacheRpcCommand command = new RemoveCacheCommand(cacheName, null, null, null);

      log.debugf("Testing " + command.getClass().getCanonicalName());
      Message nonOobRequest = serialize(command, false);
      if (nonOobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(nonOobRequest, null);
      Assert.assertFalse(executorService.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");
   }

   public void testSingleRpcCommandWithBlockingCommand() throws Exception {
      ReplicableCommand other = getReplicableCommand(true);
      CacheRpcCommand command = new SingleRpcCommand(cacheName, other);
      //single rpc command has a blocking command, so it should be dispatched to the thread pool
      assertDispatchForCommand(command, true);
   }

   public void testSingleRpcCommandWithNonBlockingCommand() throws Exception {
      ReplicableCommand other = getReplicableCommand(false);
      CacheRpcCommand command = new SingleRpcCommand(cacheName, other);
      //single rpc command does *not* have a blocking command, so it should *not* be dispatched to the thread pool
      assertDispatchForCommand(command, false);
   }

   public void testMultipleRpcCommandWithNonBlockingCommands() throws Exception {
      ReplicableCommand other1 = getReplicableCommand(false);
      ReplicableCommand other2 = getReplicableCommand(false);
      CacheRpcCommand command = new MultipleRpcCommand(Arrays.asList(other1, other2), cacheName);
      //multiple rpc command does *not* have a blocking command, so it should *not* be dispatched to the thread pool
      assertDispatchForCommand(command, false);
   }

   public void testMultipleRpcCommandWithBlockingCommands() throws Exception {
      ReplicableCommand other1 = getReplicableCommand(true);
      ReplicableCommand other2 = getReplicableCommand(true);
      CacheRpcCommand command = new MultipleRpcCommand(Arrays.asList(other1, other2), cacheName);
      //multiple rpc command has a blocking command, so it should be dispatched to the thread pool
      assertDispatchForCommand(command, true);
   }

   public void testMultipleRpcCommandWithBlockingAndNonBlockingCommand() throws Exception {
      ReplicableCommand other1 = getReplicableCommand(true);
      ReplicableCommand other2 = getReplicableCommand(false);
      CacheRpcCommand command = new MultipleRpcCommand(Arrays.asList(other1, other2), cacheName);
      //multiple rpc command has at least one blocking command, so it should be dispatched to the thread pool
      assertDispatchForCommand(command, true);
   }

   public void testNonCacheRpcCommands() throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(ReplicableCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers()) || LocalCommand.class.isAssignableFrom(clazz) ||
               CacheRpcCommand.class.isAssignableFrom(clazz)) {
            continue;
         }

         Constructor constructor = clazz.getDeclaredConstructor();
         constructor.setAccessible(true);
         ReplicableCommand command = (ReplicableCommand) constructor.newInstance();

         assertDispatchForCommand(command);
      }
   }
   
   private void assertDispatchForCommand(ReplicableCommand command) throws Exception {
      log.debugf("Testing " + command.getClass().getCanonicalName());
      commandsFactory.initializeReplicableCommand(command, true);
      Message oobRequest = serialize(command, true);
      if (oobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(oobRequest, null);
      Assert.assertEquals(executorService.hasExecutedCommand, command.canBlock(),
                          "Command " + command.getClass() + " dispatched wrongly.");

      Message nonOobRequest = serialize(command, false);
      if (nonOobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(nonOobRequest, null);
      Assert.assertFalse(executorService.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");
   }

   private void assertDispatchForCommand(ReplicableCommand command, boolean expected) throws Exception {
      log.debugf("Testing " + command.getClass().getCanonicalName());
      commandsFactory.initializeReplicableCommand(command, true);
      Message oobRequest = serialize(command, true);
      if (oobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(oobRequest, null);
      Assert.assertEquals(executorService.hasExecutedCommand, expected,
                          "Command " + command.getClass() + " dispatched wrongly.");

      Message nonOobRequest = serialize(command, false);
      if (nonOobRequest == null) {
         log.debugf("Don't test " + command.getClass() + ". it is not Serializable");
         return;
      }
      executorService.reset();
      commandAwareRpcDispatcher.handle(nonOobRequest, null);
      Assert.assertFalse(executorService.hasExecutedCommand, "Command " + command.getClass() + " dispatched wrongly.");
   }

   private Message serialize(ReplicableCommand command, boolean oob) {
      Buffer buffer;
      try {
         buffer = marshaller.objectToBuffer(command);
      } catch (Exception e) {
         //ignore, it will not be replicated
         return null;
      }
      Message message = new Message(null, address, buffer.getBuf(), buffer.getOffset(), buffer.getLength());
      if (oob) {
         message.setFlag(Message.Flag.OOB);
      }
      return message;
   }

   private ReplicableCommand getReplicableCommand(boolean blocking) throws Exception {
      List<Class<?>> cacheRpcCommandList = ClassFinder.isAssignableFrom(ReplicableCommand.class);

      for (Class<?> clazz : cacheRpcCommandList) {
         if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers()) || !DataCommand.class.isAssignableFrom(clazz)) {
            continue;
         }

         try {
            ReplicableCommand command = (ReplicableCommand) (CacheRpcCommand.class.isAssignableFrom(clazz) ?
                                                                   clazz.getConstructor(String.class).newInstance(cacheName) :
                                                                   clazz.newInstance());
            if ((blocking && command.canBlock()) || (!blocking && !command.canBlock())) {
               return command;
            }
         } catch (Exception e) {
            //no-op, try next one
         }
      }
      Assert.fail("Cannot find a " + (blocking ? "blocking" : "non-blocking") + " replicable command");
      return null;
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

   private class DummyTaskCountExecutorService implements ExecutorService {

      private volatile boolean hasExecutedCommand;

      @Override
      public void shutdown() {/*no-op*/}

      @Override
      public List<Runnable> shutdownNow() {
         return Collections.emptyList();
      }

      @Override
      public boolean isShutdown() {
         return false;
      }

      @Override
      public boolean isTerminated() {
         return false;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
         return true;
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
         return null; //no-op
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
         return null; //no-op
      }

      @Override
      public Future<?> submit(Runnable task) {
         return null; //no-op
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
         return null; //no-op
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
         return null; //no-op
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
         return null; //no-op
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
         return null; //no-op
      }

      @Override
      public void execute(Runnable command) {
         hasExecutedCommand = true;
      }

      public void reset() {
         hasExecutedCommand = false;
      }
   }
}
