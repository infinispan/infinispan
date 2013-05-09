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

package org.infinispan.tx.totalorder.simple.repl;

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
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.FullAsyncTotalOrderTest")
public class FullAsyncTotalOrderTest extends MultipleCacheManagersTest {

   protected static final String KEY_1 = "key_1";
   protected static final String KEY_2 = "key_2";
   protected static final String KEY_3 = "key_3";
   protected static final String VALUE_1 = "value_1";
   protected static final String VALUE_2 = "value_2";
   protected static final String VALUE_3 = "value_3";
   protected static final String VALUE_4 = "value_4";
   protected final int clusterSize;
   protected final CacheMode mode;
   protected final boolean syncCommit;
   protected final boolean writeSkew;
   protected final boolean useSynchronization;

   public FullAsyncTotalOrderTest() {
      this(3, CacheMode.REPL_ASYNC, false, false, false);
   }

   protected FullAsyncTotalOrderTest(int clusterSize, CacheMode mode, boolean syncCommit, boolean writeSkew, boolean useSynchronization) {
      this.clusterSize = clusterSize;
      this.mode = mode;
      this.syncCommit = syncCommit;
      this.writeSkew = writeSkew;
      this.useSynchronization = useSynchronization;
   }

   public void testInterceptorChain() {
      InterceptorChain ic = advancedCache(0).getComponentRegistry().getComponent(InterceptorChain.class);
      assertTrue(ic.containsInterceptorType(TotalOrderInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderDistributionInterceptor.class));
      assertFalse(ic.containsInterceptorType(OptimisticLockingInterceptor.class));
      assertFalse(ic.containsInterceptorType(PessimisticLockingInterceptor.class));
   }

   public void testToCacheIsTransactional() {
      assertTrue(cache(0).getCacheConfiguration().transaction().autoCommit());
      assertTrue(cache(0).getCacheConfiguration().transaction().transactionMode().isTransactional());
   }

   public void testSinglePhaseTotalOrder() {
      assertTrue(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }

   public void testPut() {
      assertEmpty(KEY_1, KEY_2, KEY_3);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(0, KEY_1, VALUE_1);

      Map<Object, Object> map = new HashMap<Object, Object>();
      map.put(KEY_2, VALUE_2);
      map.put(KEY_3, VALUE_3);

      cache(0).putAll(map);

      assertCacheValue(0, KEY_1, VALUE_1);
      assertCacheValue(0, KEY_2, VALUE_2);
      assertCacheValue(0, KEY_3, VALUE_3);

      assertNoTransactions();
   }

   public void removeTest() {
      assertEmpty(KEY_1);

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(1, KEY_1, VALUE_1);

      cache(0).remove(KEY_1);

      assertCacheValue(0, KEY_1, null);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(0, KEY_1, VALUE_1);

      cache(0).remove(KEY_1);

      assertCacheValue(0, KEY_1, null);

      assertNoTransactions();
   }

   public void testPutIfAbsent() {
      assertEmpty(KEY_1, KEY_2);

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(1, KEY_1, VALUE_1);

      cache(0).putIfAbsent(KEY_1, VALUE_2);

      assertCacheValue(0, KEY_1, VALUE_1);

      cache(1).put(KEY_1, VALUE_3);

      assertCacheValue(1, KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_1, VALUE_4);

      assertCacheValue(0, KEY_1, VALUE_3);

      cache(0).putIfAbsent(KEY_2, VALUE_1);

      assertCacheValue(0, KEY_2, VALUE_1);

      assertNoTransactions();
   }

   public void testRemoveIfPresent() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(0, KEY_1, VALUE_1);

      cache(1).put(KEY_1, VALUE_2);

      assertCacheValue(1, KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_1);

      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).remove(KEY_1, VALUE_2);

      assertCacheValue(0, KEY_1, null);

      assertNoTransactions();
   }

   public void testClear() {
      assertEmpty(KEY_1);

      cache(0).put(KEY_1, VALUE_1);

      assertCacheValue(0, KEY_1, VALUE_1);

      cache(0).clear();

      assertCacheValue(0, KEY_1, null);

      assertNoTransactions();
   }

   public void testReplace() {
      assertEmpty(KEY_1);

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(1, KEY_1, VALUE_1);

      Assert.assertEquals(cache(0).replace(KEY_1, VALUE_2), VALUE_1);

      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).put(KEY_1, VALUE_3);

      assertCacheValue(0, KEY_1, VALUE_3);

      cache(0).replace(KEY_1, VALUE_3);

      assertCacheValue(0, KEY_1, VALUE_3);

      cache(0).put(KEY_1, VALUE_4);

      assertCacheValue(0, KEY_1, VALUE_4);

      assertNoTransactions();
   }

   public void testReplaceWithOldVal() {
      assertEmpty(KEY_1);

      cache(1).put(KEY_1, VALUE_1);

      assertCacheValue(1, KEY_1, VALUE_1);

      cache(0).put(KEY_1, VALUE_2);

      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_3, VALUE_4);

      assertCacheValue(0, KEY_1, VALUE_2);

      cache(0).replace(KEY_1, VALUE_2, VALUE_4);

      assertCacheValue(0, KEY_1, VALUE_4);

      assertNoTransactions();
   }

   public void testRemoveUnexistingEntry() {
      assertEmpty(KEY_1);

      cache(0).remove(KEY_1);

      assertCacheValue(0, KEY_1, null);

      assertNoTransactions();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(mode, true);
      dcc.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER).syncCommitPhase(syncCommit)
            .syncRollbackPhase(syncCommit);
      dcc.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(writeSkew);
      dcc.transaction().useSynchronization(useSynchronization);
      if (writeSkew) {
         dcc.versioning().enable().scheme(VersioningScheme.SIMPLE);
      }
      dcc.transaction().recovery().disable();
      createCluster(dcc, clusterSize);
      waitForClusterToForm();
   }

   protected void assertEmpty(Object... keys) {
      for (Cache cache : caches()) {
         for (Object key : keys) {
            assertNull(cache.get(key));
         }
      }
   }

   //originatorIndex == cache which executed the transaction
   protected void assertCacheValue(int originatorIndex, Object key, Object value) {
      for (int index = 0; index < caches().size(); ++index) {
         if ((index == originatorIndex && mode.isSynchronous()) ||
               (index != originatorIndex && syncCommit)) {
            assertEquals(index, key, value);
         } else {
            assertEventuallyEquals(index, key, value);
         }
      }

   }

   private void assertEquals(int index, Object key, Object value) {
      assert value == null ? value == cache(index).get(key) : value.equals(cache(index).get(key));
   }
}
