package org.infinispan.xsite.backupfailure.tx;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.configuration.cache.CacheMode.REPL_SYNC;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.TestingUtil.getTransactionTable;
import static org.infinispan.transaction.LockingMode.OPTIMISTIC;
import static org.infinispan.transaction.LockingMode.PESSIMISTIC;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupConfigurationBuilder;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.testing.ExceptionRunnable;
import org.infinispan.testing.Exceptions;
import org.infinispan.transaction.LockingMode;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.ClusteredCacheBackupReceiver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;

@Test(groups = "xsite", testName = "xsite.backupfailure.tx.BackupTxFailureTest")
public class BackupTxFailureTest extends AbstractMultipleSitesTest {

   private static final String CACHE_A = "REPL_1PC_OTP";
   private static final String CACHE_B = "REPL_2PC_OTP";
   private static final String CACHE_C = "REPL_1PC_PES";
   private static final String CACHE_D = "REPL_2PC_PES";
   private static final String CACHE_E = "DIST_1PC_OTP";
   private static final String CACHE_F = "DIST_2PC_OTP";
   private static final String CACHE_G = "DIST_1PC_PES";
   private static final String CACHE_H = "DIST_2PC_PES";

   private static final List<String> ALL_CACHES = List.of(CACHE_A, CACHE_B, CACHE_C, CACHE_D, CACHE_E, CACHE_F, CACHE_G, CACHE_H);
   private static final List<String> OPT_PC_CACHES = List.of(CACHE_A, CACHE_B, CACHE_E, CACHE_F);

   @DataProvider(name = "all-caches")
   public static Object[][] allCaches() {
      return ALL_CACHES.stream()
            .map(s -> new Object[]{s})
            .toArray(Object[][]::new);
   }

   @DataProvider(name = "opt-caches")
   public static Object[][] optimisticCaches() {
      return OPT_PC_CACHES.stream()
            .map(s -> new Object[]{s})
            .toArray(Object[][]::new);
   }

   @Override
   protected void afterSitesCreated() {
      super.afterSitesCreated();

      defineInSite(site(0), CACHE_A, configFor(REPL_SYNC, OPTIMISTIC, false, siteName(1)));
      defineInSite(site(1), CACHE_A, configFor(REPL_SYNC, OPTIMISTIC, false, siteName(0)));
      defineInSite(site(0), CACHE_B, configFor(REPL_SYNC, OPTIMISTIC, true, siteName(1)));
      defineInSite(site(1), CACHE_B, configFor(REPL_SYNC, OPTIMISTIC, true, siteName(0)));
      defineInSite(site(0), CACHE_C, configFor(REPL_SYNC, PESSIMISTIC, false, siteName(1)));
      defineInSite(site(1), CACHE_C, configFor(REPL_SYNC, PESSIMISTIC, false, siteName(0)));
      defineInSite(site(0), CACHE_D, configFor(REPL_SYNC, PESSIMISTIC, true, siteName(1)));
      defineInSite(site(1), CACHE_D, configFor(REPL_SYNC, PESSIMISTIC, true, siteName(0)));

      defineInSite(site(0), CACHE_E, configFor(DIST_SYNC, OPTIMISTIC, false, siteName(1)));
      defineInSite(site(1), CACHE_E, configFor(DIST_SYNC, OPTIMISTIC, false, siteName(0)));
      defineInSite(site(0), CACHE_F, configFor(DIST_SYNC, OPTIMISTIC, true, siteName(1)));
      defineInSite(site(1), CACHE_F, configFor(DIST_SYNC, OPTIMISTIC, true, siteName(0)));
      defineInSite(site(0), CACHE_G, configFor(DIST_SYNC, PESSIMISTIC, false, siteName(1)));
      defineInSite(site(1), CACHE_G, configFor(DIST_SYNC, PESSIMISTIC, false, siteName(0)));
      defineInSite(site(0), CACHE_H, configFor(DIST_SYNC, PESSIMISTIC, true, siteName(1)));
      defineInSite(site(1), CACHE_H, configFor(DIST_SYNC, PESSIMISTIC, true, siteName(0)));

      for (var name : ALL_CACHES) {
         site(0).waitForClusterToForm(name);
         site(1).waitForClusterToForm(name);
      }
   }

   private Configuration configFor(CacheMode cacheMode, LockingMode lockingMode, boolean useTwoPhaseCommit, String backup) {
      var builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.transaction().lockingMode(lockingMode);
      builder.clustering().hash().numSegments(20);
      var sitesBuilder = builder.sites().addBackup();
      sitesBuilder
            .site(backup)
            .strategy(BackupConfiguration.BackupStrategy.SYNC)
            .backupFailurePolicy(BackupFailurePolicy.FAIL)
            .useTwoPhaseCommit(useTwoPhaseCommit);
      decorate(sitesBuilder);
      return builder.build();
   }

   protected void decorate(BackupConfigurationBuilder builder) {
      // to be overwritten
   }

   protected void assertAfterTest(Cache<String, String> cache) {

   }

   @Test(dataProvider = "all-caches")
   public void testFailDuringBackupReplay(String cacheName) {
      Cache<String, String> localCache = cache(0, 0, cacheName);
      Cache<String, String> remoteCache = cache(1, 0, cacheName);

      // put the initial value
      AssertCondition<String, String> condition = cache -> assertEquals("initial", cache.get("key"));
      localCache.put("key", "initial");
      // sync replication -> should be available immediately
      assertInAllSitesAndCaches(cacheName, condition);

      try (var failureInterceptor = failureInterceptor(remoteCache)) {
         // fail on PutKeyValueCommand
         failureInterceptor.enable(FailureEvent.WRITE);
         Exceptions.expectException(CacheException.class, RollbackException.class, () -> localCache.put("key", "wrong"));
      }

      // make sure the key is not committed anywhere!!
      assertInAllSitesAndCaches(cacheName, condition);
      assertNoTransaction(cacheName);
      assertAfterTest(localCache);
   }

   @Test(dataProvider = "all-caches")
   public void testFailDuringBackupPrepare(String cacheName) {
      Cache<String, String> localCache = cache(0, 0, cacheName);
      Cache<String, String> remoteCache = cache(1, 0, cacheName);

      // put the initial value
      AssertCondition<String, String> condition = cache -> assertEquals("initial", cache.get("key"));
      localCache.put("key", "initial");
      // sync replication -> should be available immediately
      assertInAllSitesAndCaches(cacheName, condition);

      try (var failureInterceptor = failureInterceptor(remoteCache)) {
         // fail on PrepareCommand
         failureInterceptor.enable(FailureEvent.PREPARE);
         Exceptions.expectException(CacheException.class, RollbackException.class, () -> localCache.put("key", "wrong"));
      }

      // make sure the key is not committed anywhere!!
      assertInAllSitesAndCaches(cacheName, condition);
      assertNoTransaction(cacheName);
      assertAfterTest(localCache);
   }

   @Test(dataProvider = "opt-caches")
   public void testFailDuringLocalPrepare(String cacheName) {
      // only test with optimistic locking
      // other combination will break data consistency even in single cluster.
      Cache<String, String> localCache = cache(0, 0, cacheName);
      Cache<String, String> otherLocalCAche = cache(0, 1, cacheName);

      // put the initial value
      AssertCondition<String, String> condition = cache -> assertEquals("initial", cache.get("key"));
      localCache.put("key", "initial");
      // sync replication -> should be available immediately
      assertInAllSitesAndCaches(cacheName, condition);

      try (var failureInterceptor = failureInterceptor(otherLocalCAche)) {
         // fail on PrepareCommand
         failureInterceptor.enable(FailureEvent.PREPARE);
         Exceptions.expectException(CacheException.class, RollbackException.class, () -> localCache.put("key", "wrong"));
      }

      // make sure the key is not committed anywhere!!
      assertInAllSitesAndCaches(cacheName, condition);
      assertNoTransaction(cacheName);
   }

   @Test(dataProvider = "all-caches")
   public void testConcurrency(String cacheName) throws ExecutionException, InterruptedException, TimeoutException {
      // the test does not fit the class but this class has a lot of cache with different combination already available
      // and ready to use.

      Cache<String, Integer> site0 = cache(0, 0, cacheName);
      Cache<String, Integer> site1 = cache(1, 0, cacheName);

      site0.put("counter", 0);
      var latch = new CountDownLatch(1);
      var maxUpdates = 10;
      var c1 = new CounterRunnable(latch, site0.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK), maxUpdates);
      var c2 = new CounterRunnable(latch, site1.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT), maxUpdates);

      Future<Void> f1 = fork(c1);
      // use zero-lock timeout to "solve" deadlocks faster
      Future<Void> f2 = fork(c2);

      latch.countDown();

      f1.get(10, TimeUnit.SECONDS);
      f2.get(10, TimeUnit.SECONDS);

      // assert unique updates
      var updates = IntSets.concurrentSet(maxUpdates * 2);
      updates.addAll(c1.addedValues);
      for (var i : c2.addedValues) {
         assertTrue("concurrent update detected: " + c1.addedValues + " - " + c2.addedValues, updates.add(i));
      }

      assertNoTransaction(cacheName);

   }

   private FailureInterceptor failureInterceptor(Cache<String, String> cache) {
      var chain = extractInterceptorChain(cache);
      synchronized (chain) {
         var interceptor = chain.findInterceptorWithClass(FailureInterceptor.class);
         if (interceptor != null) {
            return interceptor;
         }
         interceptor = new FailureInterceptor();
         chain.addInterceptor(interceptor, 1);
         return interceptor;
      }
   }

   private void assertNoTransaction(String cacheName) {
      eventuallyAssertInAllSitesAndCaches(cacheName, cache -> getTransactionTable(cache).getLocalTransactions().isEmpty());
      eventuallyAssertInAllSitesAndCaches(cacheName, cache -> getTransactionTable(cache).getRemoteTransactions().isEmpty());
      eventuallyAssertInAllSitesAndCaches(cacheName, cache -> backupReceiver(cache).isTransactionTableEmpty());
   }

   private static ClusteredCacheBackupReceiver backupReceiver(Cache<?, ?> cache) {
      var receiver = extractComponent(cache, BackupReceiver.class);
      assertTrue(receiver instanceof ClusteredCacheBackupReceiver);
      return (ClusteredCacheBackupReceiver) receiver;
   }

   @SuppressWarnings("rawtypes")
   public static class FailureInterceptor extends DDAsyncInterceptor implements AutoCloseable {

      private volatile FailureEvent event;

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         failIf(FailureEvent.WRITE);
         return super.visitPutKeyValueCommand(ctx, command);
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         failIf(FailureEvent.PREPARE);
         return super.visitPrepareCommand(ctx, command);
      }

      private void failIf(FailureEvent currentEvent) {
         if (event == currentEvent) {
            throw new CacheException("Induced Exception");
         }
      }

      void enable(FailureEvent event) {
         this.event = Objects.requireNonNull(event);
      }

      void disable() {
         event = null;
      }

      @Override
      public void close() {
         disable();
      }
   }

   private static class CounterRunnable implements ExceptionRunnable {

      final CountDownLatch latch;
      final AdvancedCache<String, Integer> cache;
      final int maxUpdates;
      final IntSet addedValues;

      private CounterRunnable(CountDownLatch latch, AdvancedCache<String, Integer> cache, int maxUpdates) {
         this.latch = latch;
         this.cache = cache;
         this.maxUpdates = maxUpdates;
         addedValues = IntSets.concurrentSet(maxUpdates);
      }

      @Override
      public void run() {
         var tm = cache.getTransactionManager();
         for (int i = 0; i < maxUpdates; ++i) {
            var failed = false;
            int updatedValue = -1;
            try {
               tm.begin();
               var value = cache.get("counter");
               updatedValue = value + 1;
               cache.put("counter", updatedValue);
            } catch (Exception e) {
               failed = true;
            }

            if (failed) {
               try {
                  tm.rollback();
               } catch (SystemException e) {
                  //ignore
               }
            } else {
               try {
                  tm.commit();
               } catch (RollbackException | HeuristicMixedException | HeuristicRollbackException | SystemException e) {
                  failed = true;
               }
            }

            if (!failed) {
               assertTrue(updatedValue > 0);
               addedValues.add(updatedValue);
            }
         }
      }
   }

   private enum FailureEvent {
      WRITE, PREPARE
   }

}
