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

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.TransactionManager;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.*;

@Test(testName = "container.versioning.AbstractClusteredWriteSkewTest", groups = "functional")
public abstract class AbstractClusteredWriteSkewTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder
            .clustering()
            .cacheMode(getCacheMode())
            .versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE)
            .locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true)
            .transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .syncCommitPhase(true);

      decorate(builder);

      createCluster(builder, clusterSize());
      waitForClusterToForm();
   }

   protected void decorate(ConfigurationBuilder builder) {
      // No-op
   }

   protected abstract CacheMode getCacheMode();
   
   protected abstract int clusterSize();

   // This test is based on a contribution by Pedro Ruivo of INESC-ID, working on the Cloud-TM project.
   public void testSharedCounter() {
      int counterMaxValue = 200;
      Cache<String, Integer> c1 = cache(0);
      Cache<String, Integer> c2 = cache(1);

      //initialize the counter
      c1.put("counter", 0);

      //check if the counter is initialized in all caches
      assert c1.get("counter") == 0 : "Initial value is different from zero in cache 1";
      assert c2.get("counter") == 0 : "Initial value is different from zero in cache 2";

      //this will keep the values put by both threads. any duplicate value will be detected because of the
      //return value of add() method
      ConcurrentSkipListSet<Integer> uniqueValuesIncremented = new ConcurrentSkipListSet<Integer>();

      //create both threads (each of them incrementing the counter on one node)
      Future<Boolean> f1 = fork(new IncrementCounterTask(c1, uniqueValuesIncremented, counterMaxValue));
      Future<Boolean> f2 = fork(new IncrementCounterTask(c2, uniqueValuesIncremented, counterMaxValue));

      try {
         // wait to finish
         Boolean unique1 = f1.get(30, TimeUnit.SECONDS);
         Boolean unique2 = f2.get(30, TimeUnit.SECONDS);

         // check is any duplicate value has been detected
         assertTrue(unique1, c1.getName() + " has put a duplicate value");
         assertTrue(unique2, c2.getName() + " has put a duplicate value");
      } catch (InterruptedException e) {
         assert false : "Interrupted exception while running the test";
      } catch (ExecutionException e) {
         fail("Exception running updater threads", e);
      } catch (TimeoutException e) {
         fail("Timed out waiting for updater threads");
      } finally {
         f1.cancel(true);
         f2.cancel(true);
      }

      //check if all caches obtains the counter_max_values
      assert c1.get("counter") >= counterMaxValue : "Final value is less than " + counterMaxValue +
            " in cache 1";
      assert c2.get("counter") >= counterMaxValue : "Final value is less than " + counterMaxValue +
            " in cache 2";
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
      public Boolean call() {
         while (lastValue < counterMaxValue && !Thread.interrupted()) {
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
            } catch (Exception e) {
               try {
                  //lets rollback
                  if (transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION)
                     transactionManager.rollback();
               } catch (Throwable t) {
                  //the only possible exception is thrown by the rollback. just ignore it
                  log.trace("Exception during rollback", t);
               }
            } finally {
               assertTrue(unique, "Duplicate value found in " + address(cache) + " (value=" + lastValue + ")");
            }
         }
         return unique;
      }
   }
}
