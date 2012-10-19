/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * // TODO: Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateConsumerTest", enabled = true)
public class StateConsumerTest {

   private static final Log log = LogFactory.getLog(StateConsumerTest.class);

   public void test1() throws Exception {
      // create cache configuration
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().timeout(10000)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .locking().lockAcquisitionTimeout(200).writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ);

      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      GlobalConfiguration globalConfiguration = gcb.build();
      Configuration configuration = cb.build();

      // create list of 6 members
      List<Address> members1 = new ArrayList<Address>();
      for (int i = 0; i < 6; i++) {
         members1.add(new TestAddress(i));
      }
      List<Address> members2 = new ArrayList<Address>(members1);
      members2.remove(new TestAddress(5));
      members2.add(new TestAddress(6));

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(new MurmurHash3(), 2, 4, members1);
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2);   //todo [anistor] it seems that address 6 is not used for un-owned segments

      log.debug(ch1);
      log.debug(ch2);

      // create dependencies
      Cache cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      StateProvider stateProvider = mock(StateProvider.class);
      LocalTopologyManager localTopologyManager = mock(LocalTopologyManager.class);
      CacheNotifier cacheNotifier = mock(CacheNotifier.class);
      ExecutorService mockExecutorService = mock(ExecutorService.class);
      RpcManager rpcManager = mock(RpcManager.class);
      Transport transport = mock(Transport.class);
      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      CacheLoaderManager cacheLoaderManager = mock(CacheLoaderManager.class);
      DataContainer dataContainer = mock(DataContainer.class);
      TransactionTable transactionTable = mock(TransactionTable.class);
      StateTransferLock stateTransferLock = mock(StateTransferLock.class);
      InterceptorChain interceptorChain = mock(InterceptorChain.class);
      InvocationContextContainer icc = mock(InvocationContextContainer.class);

      when(mockExecutorService.submit(any(Runnable.class))).thenAnswer(new Answer<Future<?>>() {
         @Override
         public Future<?> answer(InvocationOnMock invocation) {
            return null;
         }
      });

      when(commandsFactory.buildStateRequestCommand(any(StateRequestCommand.Type.class), any(Address.class), anyInt(), any(Set.class))).thenAnswer(new Answer<StateRequestCommand>() {
         @Override
         public StateRequestCommand answer(InvocationOnMock invocation) {
            return new StateRequestCommand("cache1", (StateRequestCommand.Type) invocation.getArguments()[0], (Address) invocation.getArguments()[1], (Integer) invocation.getArguments()[2], (Set) invocation.getArguments()[3]);
         }
      });

      when(transport.getViewId()).thenReturn(1);
      when(rpcManager.getAddress()).thenReturn(new TestAddress(0));
      when(rpcManager.getTransport()).thenReturn(transport);

      when(rpcManager.invokeRemotely(any(Collection.class), any(ReplicableCommand.class), any(ResponseMode.class), anyLong())).thenAnswer(new Answer<Map<Address, Response>>() {
         @Override
         public Map<Address, Response> answer(InvocationOnMock invocation) {
            Collection<Address> recipients = (Collection<Address>) invocation.getArguments()[0];
            ReplicableCommand rpcCommand = (ReplicableCommand) invocation.getArguments()[1];
            if (rpcCommand instanceof StateRequestCommand) {
               StateRequestCommand cmd = (StateRequestCommand) rpcCommand;
               Map<Address, Response> results = new HashMap<Address, Response>();
               if (cmd.getType().equals(StateRequestCommand.Type.GET_TRANSACTIONS)) {
                  for (Address recipient : recipients) {
                     results.put(recipient, SuccessfulResponse.create(new ArrayList<TransactionInfo>()));
                  }
               } else if (cmd.getType().equals(StateRequestCommand.Type.START_STATE_TRANSFER) || cmd.getType().equals(StateRequestCommand.Type.CANCEL_STATE_TRANSFER)) {
                  for (Address recipient : recipients) {
                     results.put(recipient, SuccessfulResponse.SUCCESSFUL_EMPTY_RESPONSE);
                  }
               }
               return results;
            }
            return Collections.emptyMap();
         }
      });


      // create state provider
      StateConsumerImpl stateConsumer = new StateConsumerImpl();
      stateConsumer.init(cache, localTopologyManager, interceptorChain, icc, configuration, rpcManager,
            commandsFactory, cacheLoaderManager, dataContainer, transactionTable, stateTransferLock);
      stateConsumer.start();

      final List<InternalCacheEntry> cacheEntries = new ArrayList<InternalCacheEntry>();
      Object key1 = new TestKey("key1", 0, ch1);
      Object key2 = new TestKey("key2", 0, ch1);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      when(dataContainer.iterator()).thenAnswer(new Answer<Iterator<InternalCacheEntry>>() {
         @Override
         public Iterator<InternalCacheEntry> answer(InvocationOnMock invocation) {
            return cacheEntries.iterator();
         }
      });
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.<LocalTransaction>emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.<RemoteTransaction>emptyList());

      // create segments
      Set<Integer> segments = new HashSet<Integer>(Arrays.asList(0, 1, 2, 3, 4));

      Set<Integer> seg = new HashSet<Integer>(Arrays.asList(0));

      assertFalse(stateConsumer.isStateTransferInProgress());

      stateConsumer.onTopologyUpdate(new CacheTopology(1, ch1, ch1), false);

      assertTrue(stateConsumer.isStateTransferInProgress());

      stateConsumer.onTopologyUpdate(new CacheTopology(2, ch1, ch2), true);

      stateConsumer.stop();

      assertFalse(stateConsumer.isStateTransferInProgress());
   }
}
