/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.container.versioning;

import java.util.concurrent.ConcurrentSkipListSet;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "container.versioning.TransactionalLocalWriteSkewTest")
public class TransactionalLocalWriteSkewTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
            .transaction()
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.OPTIMISTIC).syncCommitPhase(true)
            .locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true)
            .versioning().enable().scheme(VersioningScheme.SIMPLE);

      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testSharedCounter() throws Exception {
      int counterMaxValue = 1000;
      Cache<String, Integer> c1 = cacheManager.getCache("cache");

      // initialize the counter
      c1.put("counter", 0);

      // check if the counter is initialized in all caches
      assertTrue(c1.get("counter") == 0);

      // this will keep the values put by both threads. any duplicate value
      // will be detected because of the
      // return value of add() method
      ConcurrentSkipListSet<Integer> uniqueValuesIncremented = new ConcurrentSkipListSet<Integer>();

      // create both threads (simulate a node)
      IncrementCounterThread ict1 = new IncrementCounterThread("Node-1", c1,
                                                               uniqueValuesIncremented, counterMaxValue);
      IncrementCounterThread ict2 = new IncrementCounterThread("Node-2", c1,
                                                               uniqueValuesIncremented, counterMaxValue);

      // start and wait to finish
      ict1.start();
      ict2.start();

      ict1.join();
      ict2.join();

      // check if all caches obtains the counter_max_values
      assertTrue(c1.get("counter") >= counterMaxValue);

      // check is any duplicate value is detected
      assertTrue(ict1.result);
      assertTrue(ict2.result);
   }

   private static class IncrementCounterThread extends Thread {
      Log log = LogFactory.getLog(IncrementCounterThread.class);

      private Cache<String, Integer> cache;

      private ConcurrentSkipListSet<Integer> uniqueValuesSet;

      private TransactionManager transactionManager;

      private int lastValue;

      private boolean result = true;

      private int counterMaxValue;

      public IncrementCounterThread(String name,
                                    Cache<String, Integer> cache,
                                    ConcurrentSkipListSet<Integer> uniqueValuesSet,
                                    int counterMaxValue) {
         super(name);
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache()
               .getTransactionManager();
         this.uniqueValuesSet = uniqueValuesSet;
         this.lastValue = 0;
         this.counterMaxValue = counterMaxValue;
      }

      @Override
      public void run() {
         while (lastValue < counterMaxValue) {
            try {
               Integer value = 0;
               try {
                  // start transaction, get the counter value, increment
                  // and put it again
                  // check for duplicates in case of success
                  transactionManager.begin();

                  value = cache.get("counter");
                  value = value + 1;
                  lastValue = value;

                  cache.put("counter", value);

                  transactionManager.commit();

                  result = result && uniqueValuesSet.add(value);
                  log.warnf("Add value=%s, result is %b", value, result);
               } catch (Throwable t) {
                  log.errorf("Exception with value=%d", value);
                  // lets rollback
                  transactionManager.rollback();
               }
            } catch (Throwable t) {
               // the only possible exception is thrown by the rollback.
               // just ignore it
            }
         }
      }
   }
}