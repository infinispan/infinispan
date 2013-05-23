/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

/**
 * A base test for all rehashing tests
 */
@Test(groups = "functional")
public abstract class RehashTestBase extends BaseDistFunctionalTest {

   protected RehashTestBase() {
      cleanup = CleanupPhase.AFTER_METHOD;
      tx = true;
      performRehashing = true;
   }

   // this setup has 4 running caches: {c1, c2, c3, c4}

   /**
    * This is overridden by subclasses.  Could typically be a JOIN or LEAVE event.
    * @param offline
    */
   abstract void performRehashEvent(boolean offline) throws Throwable;

   /**
    * Blocks until a rehash completes.
    */
   abstract void waitForRehashCompletion();

   void additionalWait() {
      TestingUtil.sleepThread(1000);
   }

   protected List<MagicKey> init() {

      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey("k1", c1), new MagicKey("k2", c2),
            new MagicKey("k3", c3), new MagicKey("k4", c4)
      ));
      assertEquals(caches.size(), keys.size(), "Received caches" + caches);

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      i = 0;
      for (MagicKey key : keys) assertOwnershipAndNonOwnership(key, false);

      log.infof("Initialized with keys %s", keys);
      return keys;
   }

   /**
    * Simple test.  Put some state, trigger event, test results
    */
   @Test
   public void testNonTransactional() throws Throwable {
      List<MagicKey> keys = init();

      log.info("Invoking rehash event");
      performRehashEvent(false);

      waitForRehashCompletion();
      log.info("Rehash complete");

      int i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);
   }


   /**
    * More complex - init some state.  Start a new transaction, and midway trigger a rehash.  Then complete transaction
    * and test results.
    */
   @Test
   public void testTransactional() throws Throwable {
      final List<MagicKey> keys = init();
      final CountDownLatch l = new CountDownLatch(1);
      final AtomicBoolean rollback = new AtomicBoolean(false);

      Thread th = new Thread("Updater") {
         @Override
         public void run() {
            try {
               // start a transaction on c1.
               TransactionManager t1 = TestingUtil.getTransactionManager(c1);
               t1.begin();
               c1.put(keys.get(0), "transactionally_replaced");
               Transaction tx = t1.getTransaction();
               tx.enlistResource(new XAResourceAdapter() {
                  public int prepare(Xid id) {
                     // this would be called *after* the cache prepares.
                     try {
                        l.await();
                     } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                     }
                     return XAResource.XA_OK;
                  }
               });
               t1.commit();
            } catch (Exception e) {
               log.error("Error committing transaction", e);
               rollback.set(true);
               throw new RuntimeException(e);
            }
         }
      };

      th.start();

      log.info("Invoking rehash event");
      performRehashEvent(true);
      l.countDown();
      th.join();

      //ownership can only be verified after the rehashing has completed
      waitForRehashCompletion();
      log.info("Rehash complete");

      //only check for these values if tx was not rolled back
      if (!rollback.get()) {
         // the ownership of k1 might change during the tx and a cache might end up with it in L1
         assertOwnershipAndNonOwnership(keys.get(0), true);
         assertOwnershipAndNonOwnership(keys.get(1), l1OnRehash);
         assertOwnershipAndNonOwnership(keys.get(2), l1OnRehash);
         assertOwnershipAndNonOwnership(keys.get(3), l1OnRehash);

         // checking the values will bring the keys to L1, so we want to do it after checking ownership
         assertOnAllCaches(keys.get(0), "transactionally_replaced");
         assertOnAllCaches(keys.get(1), "v" + 2);
         assertOnAllCaches(keys.get(2), "v" + 3);
         assertOnAllCaches(keys.get(3), "v" + 4);
      }
   }

   /**
    * A stress test.  One node is constantly modified while a rehash occurs.
    */
   @Test(enabled = false, description = "Enable after releasing Beta1")
   public void testNonTransactionalStress() throws Throwable {
      stressTest(false);
   }

   /**
    * A stress test.  One node is constantly modified using transactions while a rehash occurs.
    */
   @Test(enabled = false, description = "Enable after releasing Beta1")
   public void testTransactionalStress() throws Throwable {
      stressTest(true);
   }

   private void stressTest(boolean tx) throws Throwable {
      final List<MagicKey> keys = init();
      final CountDownLatch latch = new CountDownLatch(1);
      List<Updater> updaters = new ArrayList<Updater>(keys.size());
      for (MagicKey k : keys) {
         Updater u = new Updater(c1, k, latch, tx);
         u.start();
         updaters.add(u);
      }

      latch.countDown();

      log.info("Invoking rehash event");
      performRehashEvent(false);

      for (Updater u : updaters) u.complete();
      for (Updater u : updaters) u.join();

      waitForRehashCompletion();

      log.info("Rehash complete");

      int i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + updaters.get(i++).currentValue);
   }
}

class Updater extends Thread {
   static final Random r = new Random();
   volatile int currentValue = 0;
   MagicKey key;
   Cache cache;
   CountDownLatch latch;
   volatile boolean running = true;
   TransactionManager tm;

   Updater(Cache cache, MagicKey key, CountDownLatch latch, boolean tx) {
      super("Updater-" + key);
      this.key = key;
      this.cache = cache;
      this.latch = latch;
      if (tx) tm = TestingUtil.getTransactionManager(cache);
   }

   public void complete() {
      running = false;
   }

   @Override
   public void run() {
      while (running) {
         try {
            currentValue++;
            if (tm != null) tm.begin();
            cache.put(key, "v" + currentValue);
            if (tm != null) tm.commit();
            TestingUtil.sleepThread(r.nextInt(10) * 10);
         } catch (Exception e) {
            // do nothing?
         }
      }
   }
}
