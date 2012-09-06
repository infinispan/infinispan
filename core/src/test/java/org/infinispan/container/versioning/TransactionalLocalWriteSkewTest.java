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

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.Status;
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
import org.testng.Assert;
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
      Future ict1 = fork(new IncrementCounterTask(c1, uniqueValuesIncremented, counterMaxValue));
      Future ict2 = fork(new IncrementCounterTask(c1, uniqueValuesIncremented, counterMaxValue));

      try {
         // wait to finish
         Boolean unique1 = (Boolean) ict1.get(30, TimeUnit.SECONDS);
         Boolean unique2 = (Boolean) ict1.get(30, TimeUnit.SECONDS);

         // check is any duplicate value is detected
         assertTrue(unique1);
         assertTrue(unique2);

         // check if all caches obtains the counter_max_values
         assertTrue(c1.get("counter") >= counterMaxValue);
      } finally {
         ict1.cancel(true);
         ict2.cancel(true);
      }
   }

   private class IncrementCounterTask implements Callable<Boolean> {
      private Cache<String, Integer> cache;
      private ConcurrentSkipListSet<Integer> uniqueValuesSet;
      private TransactionManager transactionManager;
      private int lastValue;
      private boolean unique = true;
      private int counterMaxValue;

      public IncrementCounterTask(Cache<String, Integer> cache, ConcurrentSkipListSet<Integer> uniqueValuesSet, int counterMaxValue) {
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache().getTransactionManager();
         this.uniqueValuesSet = uniqueValuesSet;
         this.lastValue = 0;
         this.counterMaxValue = counterMaxValue;
      }

      @Override
      public Boolean call() throws InterruptedException {
         int failuresCounter = 0;
         while (lastValue < counterMaxValue && !Thread.interrupted()) {
            boolean success = false;
            try {
               //start transaction, get the counter value, increment and put it again
               //check for duplicates in case of success
               transactionManager.begin();

               Integer value = cache.get("counter");
               value = value + 1;
               lastValue = value;

               cache.put("counter", value);

               transactionManager.commit();

               unique = uniqueValuesSet.add(value);
               success = true;
            } catch (Exception e) {
               // expected exception
               failuresCounter++;
               Assert.assertTrue(failuresCounter < 10 * counterMaxValue, "Too many failures incrementing the counter");
            } finally {
               if (!success) {
                  try {
                     //lets rollback
                     if (transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION)
                        transactionManager.rollback();
                  } catch (Throwable t) {
                     //the only possible exception is thrown by the rollback. just ignore it
                     log.trace("Exception during rollback", t);
                  }
               }
               Assert.assertTrue(unique, "Duplicate value found (value=" + lastValue + ")");
            }
         }
         return unique;
      }
   }
}