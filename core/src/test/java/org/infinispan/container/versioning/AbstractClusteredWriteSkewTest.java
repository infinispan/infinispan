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

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Transaction;
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

   public final void testPutIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.PUT, false);
   }

   public final void testPutIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.PUT, false);
   }

   public final void testPutIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.PUT, true);
   }

   public final void testPutIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.PUT, true);
   }

   public final void testRemoveIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REMOVE, false);
   }

   public final void testRemoveIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REMOVE, false);
   }

   public final void testRemoveIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REMOVE, true);
   }

   public final void testRemoveIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REMOVE, true);
   }

   public final void testReplaceIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REPLACE, false);
   }

   public final void testReplaceIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REPLACE, false);
   }

   public final void testReplaceIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REPLACE, true);
   }

   public final void testReplaceIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REPLACE, true);
   }

   public final void testPutIfAbsentIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_PUT, false);
   }

   public final void testPutIfAbsentIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_PUT, false);
   }

   public final void testPutIfAbsentIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_PUT, true);
   }

   public final void testPutIfAbsentIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_PUT, true);
   }

   public final void testConditionalRemoveIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REMOVE, false);
   }

   public final void testConditionalRemoveIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REMOVE, false);
   }

   public final void testConditionalRemoveIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REMOVE, true);
   }

   public final void testConditionalRemoveIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REMOVE, true);
   }

   public final void testConditionalReplaceIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REPLACE, false);
   }

   public final void testConditionalReplaceIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REPLACE, false);
   }

   public final void testConditionalReplaceIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REPLACE, true);
   }

   public final void testConditionalReplaceIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REPLACE, true);
   }

   private void doIgnoreReturnValueTest(boolean executeOnPrimaryOwner, Operation operation, boolean initKey) throws Exception {
      final Object key = new MagicKey("ignore-return-value", cache(0));
      final AdvancedCache<Object, Object> c = executeOnPrimaryOwner ? advancedCache(0) : advancedCache(1);
      final TransactionManager tm = executeOnPrimaryOwner ? tm(0) : tm(1);

      for (Cache cache : caches()) {
         AssertJUnit.assertNull("wrong initial value for " + address(cache) + ".", cache.get(key));
      }

      log.debugf("Initialize the key? %s", initKey);
      if (initKey) {
         cache(0).put(key, "init");
      }

      Object finalValue = null;
      boolean rollbackExpected = false;

      log.debugf("Start the transaction and perform a %s operation", operation);
      tm.begin();
      switch (operation) {
         case PUT:
            finalValue = "v1";
            rollbackExpected = false;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).put(key, "v1");
            break;
         case REMOVE:
            finalValue = null;
            rollbackExpected = false;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).remove(key);
            break;
         case REPLACE:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).replace(key, "v1");
            break;
         case CONDITIONAL_PUT:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).putIfAbsent(key, "v1");
            break;
         case CONDITIONAL_REMOVE:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).remove(key, "init");
            break;
         case CONDITIONAL_REPLACE:
            finalValue = "v2";
            rollbackExpected = true;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).replace(key, "init", "v1");
            break;
         default:
            tm.rollback();
            AssertJUnit.fail("Unknown operation " + operation);
      }
      Transaction tx = tm.suspend();

      log.debugf("Suspend the transaction and update the key");
      c.put(key, "v2");

      log.debugf("Checking if all the keys has the same value");
      for (Cache cache : caches()) {
         AssertJUnit.assertEquals("wrong intermediate value for " + address(cache) + ".", "v2", cache.get(key));
      }

      log.debugf("It is going to try to commit the suspended transaction");
      try {
         tm.resume(tx);
         tm.commit();
         if (rollbackExpected) {
            AssertJUnit.fail("Rollback expected!");
         }
      } catch (RollbackException e) {
         if (!rollbackExpected) {
            AssertJUnit.fail("Rollback *not* expected!");
         }
         //no-op
      }

      log.debugf("So far so good. Check the key final value");
      assertNoTransactions();
      for (Cache cache : caches()) {
         AssertJUnit.assertEquals("wrong final value for " + address(cache) + ".", finalValue, cache.get(key));
      }
   }

   // This test is based on a contribution by Pedro Ruivo of INESC-ID, working on the Cloud-TM project.
   @Test(enabled = false, description = "Fails randomly, see ISPN-2264")
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
               assertTrue(failuresCounter < 10 * counterMaxValue, "Too many failures incrementing the counter");
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
               assertTrue(unique, "Duplicate value found in " + address(cache) + " (value=" + lastValue + ")");
            }
         }
         return unique;
      }
   }

   private static enum Operation {
      PUT, REMOVE, REPLACE,
      CONDITIONAL_PUT, CONDITIONAL_REMOVE, CONDITIONAL_REPLACE
   }
}
