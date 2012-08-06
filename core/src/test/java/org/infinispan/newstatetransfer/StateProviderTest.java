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

package org.infinispan.newstatetransfer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.DefaultConsistentHash;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * // TODO: Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.StateProviderTest", enabled = true)
public class StateProviderTest {

   private static final Log log = LogFactory.getLog(StateProviderTest.class);

   private Configuration configuration;

   private ExecutorService pooledExecutorService;

   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private CacheLoaderManager cacheLoaderManager;
   private DataContainer dataContainer;
   private TransactionTable transactionTable;
   private StateTransferLock stateTransferLock;


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

      rpcManager = mock(RpcManager.class);
      commandsFactory = mock(CommandsFactory.class);
      cacheLoaderManager = mock(CacheLoaderManager.class);
      dataContainer = mock(DataContainer.class);
      transactionTable = mock(TransactionTable.class);
      stateTransferLock = mock(StateTransferLock.class);
   }

   public void test1() {

      // create list of 6 members
      List<Address> members1 = new ArrayList<Address>();
      for (int i = 0; i < 6; i++) {
         members1.add(new TestAddress(i));
      }
      List<Address> members2 = new ArrayList<Address>(members1);
      members2.remove(new TestAddress(0));
      members2.remove(new TestAddress(5));
      members2.add(new TestAddress(6));

      // create CHes
      DefaultConsistentHashFactory chf = new DefaultConsistentHashFactory();
      DefaultConsistentHash ch1 = chf.create(new MurmurHash3(), 2, 4, members1);
      DefaultConsistentHash ch2 = chf.updateMembers(ch1, members2);   //todo [anistor] it seems that address 6 is not used for un-owned segments

      System.out.println("StateProviderTest.test1 " + ch1.dump());
      System.out.println("StateProviderTest.test1 " + ch2.dump());

      // create dependencies
      ExecutorService executorService2 = mock(ExecutorService.class);
      when(executorService2.submit(any(Runnable.class))).thenAnswer(new Answer<Future<?>>() {
         @Override
         public Future<?> answer(InvocationOnMock invocation) throws Throwable {
            return null;
         }
      });

      // create state provider
      StateProviderImpl stateProvider = new StateProviderImpl(executorService2,
            configuration, rpcManager, commandsFactory, cacheLoaderManager,
            dataContainer, transactionTable, stateTransferLock);

      final List<InternalCacheEntry> cacheEntries = new ArrayList<InternalCacheEntry>();
      cacheEntries.add(new ImmortalCacheEntry("key1", "value1"));
      cacheEntries.add(new ImmortalCacheEntry("key2", "value2"));
      when(dataContainer.iterator()).thenAnswer(new Answer<Iterator<InternalCacheEntry>>() {
         @Override
         public Iterator<InternalCacheEntry> answer(InvocationOnMock invocation) throws Throwable {
            return cacheEntries.iterator();
         }
      });
      when(transactionTable.getLocalTransactions()).thenReturn(Collections.<LocalTransaction>emptyList());
      when(transactionTable.getRemoteTransactions()).thenReturn(Collections.<RemoteTransaction>emptyList());

      // create segments
      Set<Integer> segments = new HashSet<Integer>();
      for (int i = 0; i < 5; i++) {
         segments.add(i);
      }

      List<TransactionInfo> transactions = stateProvider.getTransactionsForSegments(members1.get(0), 1, segments);
      assertEquals(0, transactions.size());

      InOrder stateTransferLockVerifier = inOrder(stateTransferLock);
      stateTransferLockVerifier.verify(stateTransferLock).transactionsExclusiveLock();
      stateTransferLockVerifier.verify(stateTransferLock).transactionsExclusiveUnlock();

      Set<Integer> seg = new HashSet<Integer>();
      seg.add(0);
      stateProvider.startOutboundTransfer(new TestAddress(5), 1, seg);

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.onTopologyUpdate(2, ch1);

      assertTrue(stateProvider.isStateTransferInProgress());

      stateProvider.shutdown();

      stateProvider.onTopologyUpdate(3, ch2);

      assertFalse(stateProvider.isStateTransferInProgress());

      stateProvider.shutdown();
   }

   private static class TestAddress implements Address {

      private final int addressNum;

      TestAddress(int addressNum) {
         this.addressNum = addressNum;
      }

      @Override
      public String toString() {
         return "TestAddress(" + addressNum + ')';
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestAddress that = (TestAddress) o;
         return addressNum == that.addressNum;
      }

      @Override
      public int hashCode() {
         return addressNum;
      }
   }
}
