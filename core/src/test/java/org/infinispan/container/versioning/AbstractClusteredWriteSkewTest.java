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

import javax.transaction.TransactionManager;
import java.util.concurrent.ConcurrentSkipListSet;

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
      int counterMaxValue = 1000;
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

      //create both threads (simulate a node)
      IncrementCounterThread ict1 = new IncrementCounterThread("Node-1", c1, uniqueValuesIncremented, counterMaxValue);
      IncrementCounterThread ict2 = new IncrementCounterThread("Node-2", c2, uniqueValuesIncremented, counterMaxValue);

      try {
         //start and wait to finish
         ict1.start();
         ict2.start();

         ict1.join();
         ict2.join();
      } catch (InterruptedException e) {
         assert false : "Interrupted exception while running the test";
      }

      //check if all caches obtains the counter_max_values
      assert c1.get("counter") >= counterMaxValue : "Final value is less than " + counterMaxValue +
            " in cache 1";
      assert c2.get("counter") >= counterMaxValue : "Final value is less than " + counterMaxValue +
            " in cache 2";

      //check is any duplicate value is detected
      assert ict1.result : ict1.getName() + " has put a duplicate value";
      assert ict2.result : ict2.getName() + " has put a duplicate value";
   }

   private static class IncrementCounterThread extends Thread {
      private Cache<String, Integer> cache;
      private ConcurrentSkipListSet<Integer> uniqueValuesSet;
      private TransactionManager transactionManager;
      private int lastValue;
      private boolean result = true;
      private int counterMaxValue;

      public IncrementCounterThread(String name, Cache<String, Integer> cache, ConcurrentSkipListSet<Integer> uniqueValuesSet, int counterMaxValue) {
         super(name);
         this.cache = cache;
         this.transactionManager = cache.getAdvancedCache().getTransactionManager();
         this.uniqueValuesSet = uniqueValuesSet;
         this.lastValue = 0;
         this.counterMaxValue = counterMaxValue;
      }

      @Override
      public void run() {
         while (lastValue < counterMaxValue) {
            try {
               try {
                  //start transaction, get the counter value, increment and put it again
                  //check for duplicates in case of success
                  transactionManager.begin();

                  Integer value = cache.get("counter");
                  value = value + 1;
                  lastValue = value;

                  cache.put("counter", value);

                  transactionManager.commit();

                  result = uniqueValuesSet.add(value);
               } catch (Throwable t) {
                  //lets rollback
                  transactionManager.rollback();
               }
            } catch (Throwable t) {
               //the only possible exception is thrown by the rollback. just ignore it
            } finally {
               assert result : "Duplicate value found in " + getName() + " (value=" + lastValue + ")";
            }
         }
      }
   }
}
