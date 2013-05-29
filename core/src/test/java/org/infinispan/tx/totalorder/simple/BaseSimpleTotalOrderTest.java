/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.tx.totalorder.simple;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TransactionTrackInterceptor;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.BaseSimpleTotalOrderTest")
public abstract class BaseSimpleTotalOrderTest extends MultipleCacheManagersTest {

   private static final String KEY_1 = "key_1";
   private static final String KEY_2 = "key_2";
   private static final String KEY_3 = "key_3";
   private static final String VALUE_1 = "value_1";
   private static final String VALUE_2 = "value_2";
   private static final String VALUE_3 = "value_3";
   private static final String VALUE_4 = "value_4";
   private static final int TX_TIMEOUT = 15; //seconds
   private final int clusterSize;
   private final CacheMode mode;
   private final boolean syncCommit;
   private final boolean writeSkew;
   private final boolean useSynchronization;
   private final TransactionTrackInterceptor[] transactionTrackInterceptors;
   private final int index1;
   private final int index2;

   protected BaseSimpleTotalOrderTest(int clusterSize, CacheMode mode, boolean syncCommit, boolean writeSkew,
                                      boolean useSynchronization) {
      this.clusterSize = clusterSize;
      this.mode = mode;
      this.syncCommit = syncCommit;
      this.writeSkew = writeSkew;
      this.useSynchronization = useSynchronization;
      this.transactionTrackInterceptors = new TransactionTrackInterceptor[clusterSize];
      this.index1 = 0;
      this.index2 = clusterSize > 1 ? 1 : 0;
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   public final void testInterceptorChain() {
      InterceptorChain ic = advancedCache(0).getComponentRegistry().getComponent(InterceptorChain.class);
      assertTrue(ic.containsInterceptorType(TotalOrderInterceptor.class));
      if (writeSkew) {
         assertFalse(ic.containsInterceptorType(TotalOrderDistributionInterceptor.class));
         assertTrue(ic.containsInterceptorType(TotalOrderVersionedDistributionInterceptor.class));
         assertTrue(ic.containsInterceptorType(TotalOrderVersionedEntryWrappingInterceptor.class));
      } else {
         assertTrue(ic.containsInterceptorType(TotalOrderDistributionInterceptor.class));
         assertFalse(ic.containsInterceptorType(TotalOrderVersionedDistributionInterceptor.class));
         assertFalse(ic.containsInterceptorType(TotalOrderVersionedEntryWrappingInterceptor.class));
      }
      assertFalse(ic.containsInterceptorType(OptimisticLockingInterceptor.class));
      assertFalse(ic.containsInterceptorType(PessimisticLockingInterceptor.class));
   }

   public final void testToCacheIsTransactional() {
      assertTrue(cache(0).getCacheConfiguration().transaction().transactionMode().isTransactional());
   }

   public void testSinglePhaseTotalOrder() {
      assertTrue(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }

   public final void testPut() throws InterruptedException {
      preCheckBeforeTest(KEY_1, KEY_2, KEY_3);

      assertNull(cache(index1).put(KEY_1, VALUE_1));
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertEquals(cache(index2).put(KEY_1, VALUE_2), VALUE_1);
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_2);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      cache(index1).putAll(map);
      assertTransactionSeenByEverybody(index1, true, KEY_2, KEY_3);
      assertCacheValue(KEY_2, VALUE_2);
      assertCacheValue(KEY_3, VALUE_3);

      map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_3);
      map.put(KEY_3, VALUE_2);

      cache(index2).putAll(map);
      assertTransactionSeenByEverybody(index2, true, KEY_2, KEY_3);
      assertCacheValue(KEY_2, VALUE_3);
      assertCacheValue(KEY_3, VALUE_2);

      assertNoTransactions();
   }

   public void removeTest() throws InterruptedException {
      preCheckBeforeTest(KEY_1);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertEquals(cache(index1).remove(KEY_1), VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, null);

      cache(index1).put(KEY_1, VALUE_2);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_2);

      assertEquals(cache(index2).remove(KEY_1), VALUE_2);
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   public void testPutIfAbsent() throws InterruptedException {
      preCheckBeforeTest(KEY_1, KEY_2, KEY_3);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertEquals(cache(index1).putIfAbsent(KEY_1, VALUE_2), VALUE_1);
      assertTransactionSeenByEverybody(index1, false); //the putIfAbsent is not successful and it will not be replicated
      assertCacheValue(KEY_1, VALUE_1);

      assertEquals(cache(index2).putIfAbsent(KEY_1, VALUE_3), VALUE_1);
      assertTransactionSeenByEverybody(index2, false); //the putIfAbsent is not successful and it will not be replicated
      assertCacheValue(KEY_1, VALUE_1);

      assertNull(cache(index1).putIfAbsent(KEY_2, VALUE_1));
      assertTransactionSeenByEverybody(index1, true, KEY_2);
      assertCacheValue(KEY_2, VALUE_1);

      assertNull(cache(index2).putIfAbsent(KEY_3, VALUE_1));
      assertTransactionSeenByEverybody(index2, true, KEY_3);
      assertCacheValue(KEY_3, VALUE_1);

      assertNoTransactions();
   }

   public void testRemoveIfPresent() throws InterruptedException {
      preCheckBeforeTest(KEY_1);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertFalse(cache(index1).remove(KEY_1, VALUE_2));
      assertTransactionSeenByEverybody(index1, false); //the removeIfPresent is not successful and it will not be replicated
      assertCacheValue(KEY_1, VALUE_1);

      assertTrue(cache(index1).remove(KEY_1, VALUE_1));
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, null);

      cache(index1).put(KEY_1, VALUE_3);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_3);

      assertFalse(cache(index2).remove(KEY_1, VALUE_2));
      assertTransactionSeenByEverybody(index2, false); //the removeIfPresent is not successful and it will not be replicated
      assertCacheValue(KEY_1, VALUE_3);

      assertTrue(cache(index2).remove(KEY_1, VALUE_3));
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   public void testClear() throws InterruptedException {
      preCheckBeforeTest(KEY_1);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      cache(index1).clear();
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, null);

      cache(index1).put(KEY_1, VALUE_2);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_2);

      cache(index2).clear();
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   public void testReplace() throws InterruptedException {
      preCheckBeforeTest(KEY_1);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertEquals(cache(index1).replace(KEY_1, VALUE_2), VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_2);

      assertEquals(cache(index2).replace(KEY_1, VALUE_3), VALUE_2);
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_3);

      assertNoTransactions();
   }

   public void testReplaceIfPresent() throws InterruptedException {
      preCheckBeforeTest(KEY_1, KEY_2);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertFalse(cache(index1).replace(KEY_1, VALUE_2, VALUE_3));
      assertTransactionSeenByEverybody(index1, false); //the replaceIfPresent is not successful and it will not be replicated
      assertCacheValue(KEY_1, VALUE_1);

      assertTrue(cache(index1).replace(KEY_1, VALUE_1, VALUE_4));
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_4);

      cache(index1).put(KEY_2, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_2);
      assertCacheValue(KEY_2, VALUE_1);

      assertFalse(cache(index1).replace(KEY_2, VALUE_2, VALUE_3));
      assertTransactionSeenByEverybody(index1, false); //the replaceIfPresent is not successful and it will not be replicated
      assertCacheValue(KEY_2, VALUE_1);

      assertTrue(cache(index1).replace(KEY_2, VALUE_1, VALUE_4));
      assertTransactionSeenByEverybody(index1, true, KEY_2);
      assertCacheValue(KEY_2, VALUE_4);

      assertNoTransactions();
   }

   public void testReplaceWithOldVal() throws InterruptedException {
      preCheckBeforeTest(KEY_1);

      cache(index1).put(KEY_1, VALUE_1);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      cache(index1).put(KEY_1, VALUE_2);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_2);

      assertEquals(cache(index1).replace(KEY_1, VALUE_1), VALUE_2);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      cache(index1).put(KEY_1, VALUE_3);
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_3);

      assertEquals(cache(index2).replace(KEY_1, VALUE_1), VALUE_3);
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, VALUE_1);

      assertNoTransactions();
   }

   public void testRemoveUnexistingEntry() throws InterruptedException {
      preCheckBeforeTest(KEY_1);

      assertNull(cache(index1).remove(KEY_1));
      assertTransactionSeenByEverybody(index1, true, KEY_1);
      assertCacheValue(KEY_1, null);

      assertNull(cache(index2).remove(KEY_1));
      assertTransactionSeenByEverybody(index2, true, KEY_1);
      assertCacheValue(KEY_1, null);

      assertNoTransactions();
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(mode, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER).syncCommitPhase(syncCommit)
            .syncRollbackPhase(syncCommit);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(writeSkew);
      dcc.transaction().useSynchronization(useSynchronization);
      dcc.clustering().hash().numOwners(2);
      if (writeSkew) {
         dcc.versioning().enable().scheme(VersioningScheme.SIMPLE);
      }
      dcc.transaction().recovery().disable();
      createCluster(dcc, clusterSize);
      waitForClusterToForm();
      for (int i = 0; i < clusterSize; ++i) {
         transactionTrackInterceptors[i] = TransactionTrackInterceptor.injectInCache(cache(i));
         transactionTrackInterceptors[i].reset();
      }
   }

   protected void preCheckBeforeTest(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
      for (TransactionTrackInterceptor interceptor : transactionTrackInterceptors) {
         interceptor.reset();
      }
   }

   //originatorIndex == cache which executed the transaction
   protected void assertCacheValue(Object key, Object value) {
      for (Cache cache : caches()) {
         assertEquals(cache.get(key), value, "Wrong value for cache " + address(cache) + ". key=" + key);
      }
   }

   protected abstract boolean isOwner(Cache cache, Object key);

   private void assertTransactionSeenByEverybody(int index, boolean checkAllInvolvedNodes, Object... keys)
         throws InterruptedException {
      //index == cache index that executed the transaction.
      GlobalTransaction lastExecutedTx = transactionTrackInterceptors[index].getLastExecutedTransaction();
      assertEquals(transactionTrackInterceptors[index].getExecutedTransactions().size(), 1);
      if (!checkAllInvolvedNodes) {
         Assert.assertTrue(transactionTrackInterceptors[index].awaitForLocalCompletion(lastExecutedTx, TX_TIMEOUT, TimeUnit.SECONDS),
                           "Transaction didn't complete locally in " + address(cache(index)) + ".");
         for (TransactionTrackInterceptor interceptor : transactionTrackInterceptors) {
            interceptor.reset();
         }
      } else {
         for (int i = 0; i < clusterSize; ++i) {
            if (i == index) {
               //the cache that executed the transaction needs to wait for the local termination
               assertTrue(transactionTrackInterceptors[i].awaitForLocalCompletion(lastExecutedTx, TX_TIMEOUT, TimeUnit.SECONDS),
                          "Transaction didn't complete locally in " + address(cache(i)) + ".");
            }
            for (Object key : keys) {
               if (i == index || isOwner(cache(i), key)) {
                  //we only need to check for caches that owns the keys
                  //or the cache that executed the transaction because the transaction is self deliver.
                  assertTrue(transactionTrackInterceptors[i].awaitForRemoteCompletion(lastExecutedTx, TX_TIMEOUT, TimeUnit.SECONDS),
                             "Transaction didn't arrive to " + address(cache(i)) + ". Key is " + key);
                  break;
               }
            }
            transactionTrackInterceptors[i].reset();
         }
      }
   }
}
