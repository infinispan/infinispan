package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.DefaultConsistentHash;
import org.infinispan.remoting.transport.Address;
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
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A base test for all rehashing tests
 */
@Test(groups = "functional", testName = "distribution.rehash.RehashTestBase", enabled = false)
public abstract class RehashTestBase extends BaseDistFunctionalTest {

   protected RehashTestBase() {
      cleanup = CleanupPhase.AFTER_METHOD;
      tx = true;
   }

   // this setup has 4 running caches: {c1, c2, c3, c4}

   /**
    * This is overridden by subclasses.  Could typically be a JOIN or LEAVE event.
    */
   abstract void performRehashEvent();

   /**
    * Blocks until a rehash completes.
    */
   abstract void waitForRehashCompletion();

   void additionalWait() {
      TestingUtil.sleepThread(1000);
   }

   protected int locateJoiner(Address joinerAddress) {
      for (Cache c : Arrays.asList(c1, c2, c3, c4)) {
         DefaultConsistentHash dch = getDefaultConsistentHash(c, SECONDS.toMillis(480));
         int i = 0;
         for (Address a : dch.getCaches()) {
            if (a.equals(joinerAddress)) return i;
            i++;
         }
      }
      throw new RuntimeException("Cannot locate joiner! Joiner is [" + joinerAddress + "]");
   }


   private List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey(c1, "k1"), new MagicKey(c2, "k2"),
            new MagicKey(c3, "k3"), new MagicKey(c4, "k4")
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);

      log.info("Initialized with keys {0}", keys);
      return keys;
   }

   /**
    * Simple test.  Put some state, trigger event, test results
    */
   public void testNonTransactional() {
      List<MagicKey> keys = init();

      log.info("Invoking rehash event");
      performRehashEvent();

      waitForRehashCompletion();
      log.info("Rehash complete");
      additionalWait();
      int i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + ++i);
      assertProperConsistentHashOnAllCaches();
   }


   /**
    * More complex - init some state.  Start a new transaction, and midway trigger a rehash.  Then complete transaction
    * and test results.
    */
   public void testTransactional() throws Exception {
      final List<MagicKey> keys = init();
      final CountDownLatch l = new CountDownLatch(1);

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
                     }
                     return XAResource.XA_OK;
                  }
               });
               t1.commit();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         }
      };

      th.start();

      log.info("Invoking rehash event");
      performRehashEvent();
      l.countDown();
      th.join();

      log.info("Rehash complete");
      additionalWait();

      assertOnAllCachesAndOwnership(keys.get(0), "transactionally_replaced");
      assertOnAllCachesAndOwnership(keys.get(1), "v" + 2);
      assertOnAllCachesAndOwnership(keys.get(2), "v" + 3);
      assertOnAllCachesAndOwnership(keys.get(3), "v" + 4);

      assertProperConsistentHashOnAllCaches();
   }

   /**
    * A stress test.  One node is constantly modified while a rehash occurs.
    */
   @Test(enabled = false, description = "Enable after releasing Beta1")
   public void testNonTransactionalStress() throws Exception {
      stressTest(false);
   }

   /**
    * A stress test.  One node is constantly modified using transactions while a rehash occurs.
    */
   @Test(enabled = false, description = "Enable after releasing Beta1")
   public void testTransactionalStress() throws Exception {
      stressTest(true);
   }

   private void stressTest(boolean tx) throws Exception {
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
      performRehashEvent();

      for (Updater u : updaters) u.complete();
      for (Updater u : updaters) u.join();

      waitForRehashCompletion();
      additionalWait();

      log.info("Rehash complete");

      int i = 0;
      for (MagicKey key : keys) assertOnAllCachesAndOwnership(key, "v" + updaters.get(i++).currentValue);

      assertProperConsistentHashOnAllCaches();
   }
}

class Updater extends Thread {
   static final Random r = new Random();
   volatile int currentValue = 0;
   BaseDistFunctionalTest.MagicKey key;
   Cache cache;
   CountDownLatch latch;
   volatile boolean running = true;
   TransactionManager tm;

   Updater(Cache cache, BaseDistFunctionalTest.MagicKey key, CountDownLatch latch, boolean tx) {
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
