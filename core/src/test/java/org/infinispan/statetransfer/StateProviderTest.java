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
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.rpc.RpcOptionsBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Test for StateProviderImpl.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateProviderTest")
public class StateProviderTest {

   private static final Log log = LogFactory.getLog(StateProviderTest.class);
   private static final TestAddress A = new TestAddress(0, "A");
   private static final TestAddress B = new TestAddress(1, "B");
   private static final TestAddress C = new TestAddress(2, "C");
   private static final TestAddress D = new TestAddress(3, "D");
   private static final TestAddress E = new TestAddress(4, "E");
   private static final TestAddress F = new TestAddress(5, "F");
   private static final TestAddress G = new TestAddress(6, "G");


   private Configuration configuration;

   private ExecutorService pooledExecutorService;
   private ExecutorService mockExecutorService;
   private Cache cache;

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CacheNotifier cacheNotifier;
   private CacheLoaderManager cacheLoaderManager;
   private DataContainer dataContainer;
   private TransactionTable transactionTable;
   private StateTransferLock stateTransferLock;
   private StateConsumer stateConsumer;
   private CacheTopology cacheTopology;

   @BeforeTest
   public void setUp() {
      // create cache configuration
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .clustering().stateTransfer().timeout(10000)
            .versioning().enable().scheme(VersioningScheme.SIMPLE)
            .locking().lockAcquisitionTimeout(200).writeSkewCheck(true).isolationLevel(IsolationLevel.REPEATABLE_READ);
      configuration = cb.build();

      ThreadFactory threadFactory = new ThreadFactory() {
         @Override
         public Thread newThread(Runnable r) {
            return new Thread(r);
         }
      };

      pooledExecutorService = new ThreadPoolExecutor(10, 20, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingDeque<Runnable>(), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

      mockExecutorService = mock(ExecutorService.class);
      cache = mock(Cache.class);
      when(cache.getName()).thenReturn("testCache");

      rpcManager = mock(RpcManager.class);
      commandsFactory = mock(CommandsFactory.class);
      cacheNotifier = mock(CacheNotifier.class);
      cacheLoaderManager = mock(CacheLoaderManager.class);
      dataContainer = mock(DataContainer.class);
      transactionTable = mock(TransactionTable.class);
      stateTransferLock = mock(StateTransferLock.class);
      stateConsumer = mock(StateConsumer.class);
      when(stateConsumer.getCacheTopology()).thenAnswer(new Answer<CacheTopology>() {
         @Override
         public CacheTopology answer(InvocationOnMock invocation) {
            return cacheTopology;
         }
      });
   }

   @AfterTest
   public void tearDown() {
      pooledExecutorService.shutdownNow();
   }

   public void test1() throws InterruptedException {
      int numSegments = 4;

      // create list of 6 members
      List<Address> members1 = Arrays.<Address>asList(A, B, C, D, E, F);
      List<Address> members2 = new ArrayList<Address>(members1);
      members2.remove(A);
      members2.remove(F);
      members2.add(G);

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(new MurmurHash3(), 2, numSegments, members1);
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2);

      // create dependencies
      when(mockExecutorService.submit(any(Runnable.class))).thenAnswer(new Answer<Future<?>>() {
         @Override
         public Future<?> answer(InvocationOnMock invocation) {
            return null;
         }
      });

      when(rpcManager.getAddress()).thenReturn(A);
      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class))).thenAnswer(new Answer<RpcOptionsBuilder>() {
         public RpcOptionsBuilder answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], true);
         }
      });

      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl();
      stateProvider.init(cache, mockExecutorService,
            configuration, rpcManager, commandsFactory, cacheNotifier, cacheLoaderManager,
            dataContainer, transactionTable, stateTransferLock, stateConsumer);

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

      cacheTopology = new CacheTopology(1, ch1, ch1);
      stateProvider.onTopologyUpdate(cacheTopology, false);

      log.debug("ch1: " + ch1);
      Set<Integer> segmentsToRequest = ch1.getSegmentsForOwner(members1.get(0));
      List<TransactionInfo> transactions = stateProvider.getTransactionsForSegments(members1.get(0), 1, segmentsToRequest);
      assertEquals(0, transactions.size());

      try {
         stateProvider.getTransactionsForSegments(members1.get(0), 1, new HashSet<Integer>(Arrays.asList(2, numSegments)));
         fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
         // expected
      }

      verifyNoMoreInteractions(stateTransferLock);

      stateProvider.startOutboundTransfer(F, 1, Collections.singleton(0));

      assertTrue(stateProvider.isStateTransferInProgress());

      log.debug("ch2: " + ch2);
      cacheTopology = new CacheTopology(2, ch2, ch2);
      stateProvider.onTopologyUpdate(cacheTopology, true);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.startOutboundTransfer(D, 1, Collections.singleton(0));

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.stop();

      assertFalse(stateProvider.isStateTransferInProgress());
   }

   public void test2() throws InterruptedException {
      int numSegments = 4;

      // create list of 6 members
      List<Address> members1 = Arrays.<Address>asList(A, B, C, D, E, F);
      List<Address> members2 = new ArrayList<Address>(members1);
      members2.remove(A);
      members2.remove(F);
      members2.add(G);

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(new MurmurHash3(), 2, numSegments, members1);
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2);   //todo [anistor] it seems that address 6 is not used for un-owned segments

      when(commandsFactory.buildStateResponseCommand(any(Address.class), anyInt(), any(Collection.class))).thenAnswer(new Answer<StateResponseCommand>() {
         @Override
         public StateResponseCommand answer(InvocationOnMock invocation) {
            return new StateResponseCommand("testCache", (Address) invocation.getArguments()[0],
                  ((Integer) invocation.getArguments()[1]).intValue(),
                  (Collection<StateChunk>) invocation.getArguments()[2]);
         }
      });

      // create dependencies
      when(rpcManager.getAddress()).thenReturn(A);

      //rpcManager.invokeRemotelyInFuture(Collections.singleton(destination), cmd, false, sendFuture, timeout);
      doAnswer(new Answer<Map<Address, Response>>() {
         @Override
         public Map<Address, Response> answer(InvocationOnMock invocation) {
            Collection<Address> recipients = (Collection<Address>) invocation.getArguments()[0];
            ReplicableCommand rpcCommand = (ReplicableCommand) invocation.getArguments()[1];
            if (rpcCommand instanceof StateResponseCommand) {
               Map<Address, Response> results = new HashMap<Address, Response>();
               TestingUtil.sleepThread(10000, "RpcManager mock interrupted during invokeRemotelyInFuture(..)");
               return results;
            }
            return Collections.emptyMap();
         }
      }).when(rpcManager).invokeRemotelyInFuture(any(Collection.class), any(ReplicableCommand.class), any(RpcOptions.class),
                                                 any(NotifyingNotifiableFuture.class));

      when(rpcManager.getRpcOptionsBuilder(any(ResponseMode.class))).thenAnswer(new Answer<RpcOptionsBuilder>() {
         public RpcOptionsBuilder answer(InvocationOnMock invocation) {
            Object[] args = invocation.getArguments();
            return new RpcOptionsBuilder(10000, TimeUnit.MILLISECONDS, (ResponseMode) args[0], true);
         }
      });


      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl();
      stateProvider.init(cache, pooledExecutorService,
            configuration, rpcManager, commandsFactory, cacheNotifier, cacheLoaderManager,
            dataContainer, transactionTable, stateTransferLock, stateConsumer);

      final List<InternalCacheEntry> cacheEntries = new ArrayList<InternalCacheEntry>();
      Object key1 = new TestKey("key1", 0, ch1);
      Object key2 = new TestKey("key2", 0, ch1);
      Object key3 = new TestKey("key3", 1, ch1);
      Object key4 = new TestKey("key4", 1, ch1);
      cacheEntries.add(new ImmortalCacheEntry(key1, "value1"));
      cacheEntries.add(new ImmortalCacheEntry(key2, "value2"));
      cacheEntries.add(new ImmortalCacheEntry(key3, "value3"));
      cacheEntries.add(new ImmortalCacheEntry(key4, "value4"));
      when(dataContainer.iterator()).thenAnswer(new Answer<Iterator<InternalCacheEntry>>() {
         @Override
         public Iterator<InternalCacheEntry> answer(InvocationOnMock invocation) {
            return cacheEntries.iterator();
         }
      });
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.<LocalTransaction>emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.<RemoteTransaction>emptyList());

      cacheTopology = new CacheTopology(1, ch1, ch1);
      stateProvider.onTopologyUpdate(cacheTopology, false);

      log.debug("ch1: " + ch1);
      Set<Integer> segmentsToRequest = ch1.getSegmentsForOwner(members1.get(0));
      List<TransactionInfo> transactions = stateProvider.getTransactionsForSegments(members1.get(0), 1, segmentsToRequest);
      assertEquals(0, transactions.size());

      try {
         stateProvider.getTransactionsForSegments(members1.get(0), 1, new HashSet<Integer>(Arrays.asList(2, numSegments)));
         fail("IllegalArgumentException expected");
      } catch (IllegalArgumentException e) {
         // expected
      }

      verifyNoMoreInteractions(stateTransferLock);

      stateProvider.startOutboundTransfer(F, 1, Collections.singleton(0));

      assertTrue(stateProvider.isStateTransferInProgress());

      // TestingUtil.sleepThread(15000);
      log.debug("ch2: " + ch2);
      cacheTopology = new CacheTopology(2, ch2, ch2);
      stateProvider.onTopologyUpdate(cacheTopology, false);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.startOutboundTransfer(E, 1, Collections.singleton(0));

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.stop();

      assertFalse(stateProvider.isStateTransferInProgress());
   }
}
