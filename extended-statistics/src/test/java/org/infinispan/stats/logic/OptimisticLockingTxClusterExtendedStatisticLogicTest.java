package org.infinispan.stats.logic;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.CacheStatisticCollector;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.stats.wrappers.ExtendedStatisticLockManager;
import org.infinispan.stats.wrappers.ExtendedStatisticRpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.TransactionTrackInterceptor;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.CacheStatisticCollector.convertNanosToMicro;
import static org.infinispan.stats.container.ExtendedStatistic.*;
import static org.infinispan.test.TestingUtil.*;
import static org.testng.Assert.*;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "stats.logic.OptimisticLockingTxClusterExtendedStatisticLogicTest")
public class OptimisticLockingTxClusterExtendedStatisticLogicTest extends MultipleCacheManagersTest {

   private static final Object KEY_1 = "KEY_1";
   private static final Object VALUE_1 = "VALUE_1";
   private static final Object VALUE_2 = "VALUE_2";
   private static final Object VALUE_3 = "VALUE_3";
   private static final int NUM_NODES = 2;
   private static final int TX_TIMEOUT = 60;
   private static final TimeService TEST_TIME_SERVICE = new DefaultTimeService() {
      @Override
      public long time() {
         return 0;
      }

      @Override
      public long timeDuration(long startTime, TimeUnit outputTimeUnit) {
         assertEquals(startTime, 0, "Start timestamp must be zero!");
         assertEquals(outputTimeUnit, NANOSECONDS, "TimeUnit is different from expected");
         return 1;
      }

      @Override
      public long timeDuration(long startTime, long endTime, TimeUnit outputTimeUnit) {
         assertEquals(startTime, 0, "Start timestamp must be zero!");
         assertEquals(endTime, 0, "End timestamp must be zero!");
         assertEquals(outputTimeUnit, NANOSECONDS, "TimeUnit is different from expected");
         return 1;
      }
   };
   private static final double MICROSECONDS = convertNanosToMicro(TEST_TIME_SERVICE.timeDuration(0, NANOSECONDS));
   private final ExtendedStatisticInterceptor[] extendedStatisticInterceptors = new ExtendedStatisticInterceptor[NUM_NODES];
   private final TransactionTrackInterceptor[] transactionTrackInterceptors = new TransactionTrackInterceptor[NUM_NODES];
   private final LockManager[] lockManagers = new LockManager[NUM_NODES];
   private final ControlledRpcManager[] controlledRpcManager = new ControlledRpcManager[NUM_NODES];
   private final LockManagerTimeService lockManagerTimeService = new LockManagerTimeService();

   public void testLockingTimeoutOnOwnerWithLocalTx() throws Exception {
      doTimeoutTest(true, false);
   }

   public void testLockingTimeoutOnNonOwnerWithLocalTx() throws Exception {
      doTimeoutTest(false, false);
   }

   public void testLockingTimeoutOnOwnerWithRemoteTx() throws Exception {
      doTimeoutTest(true, true);
   }

   public void testLockingTimeoutOnNonOwnerWithRemoteTx() throws Exception {
      doTimeoutTest(false, true);
   }

   public void testWriteSkewOnOwner() throws Exception {
      doWriteSkewTest(true);
   }

   @Test (groups = "unstable", description = "https://issues.jboss.org/browse/ISPN-3341")
   public void testWriteSkewOnNonOwner() throws Exception {
      doWriteSkewTest(false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true)
               .lockAcquisitionTimeout(1000);
         builder.transaction().recovery().disable();
         builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
         builder.deadlockDetection().enable();
         builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
         extendedStatisticInterceptors[i] = new ExtendedStatisticInterceptor();
         builder.customInterceptors().addInterceptor().interceptor(extendedStatisticInterceptors[i])
               .after(TxInterceptor.class);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
      for (int i = 0; i < NUM_NODES; ++i) {
         ExtendedStatisticInterceptor interceptor = extendedStatisticInterceptors[i];
         CacheStatisticManager manager = (CacheStatisticManager) extractField(interceptor, "cacheStatisticManager");
         CacheStatisticCollector collector = (CacheStatisticCollector) extractField(manager, "cacheStatisticCollector");
         ConcurrentGlobalContainer globalContainer = (ConcurrentGlobalContainer) extractField(collector, "globalContainer");
         ExtendedStatisticRpcManager rpcManager = (ExtendedStatisticRpcManager) extractComponent(cache(i), RpcManager.class);
         ExtendedStatisticLockManager lockManager = (ExtendedStatisticLockManager) extractLockManager(cache(i));
         lockManagers[i] = lockManager;
         replaceField(TEST_TIME_SERVICE, "timeService", manager, CacheStatisticManager.class);
         replaceField(TEST_TIME_SERVICE, "timeService", collector, CacheStatisticCollector.class);
         replaceField(TEST_TIME_SERVICE, "timeService", globalContainer, ConcurrentGlobalContainer.class);
         replaceField(TEST_TIME_SERVICE, "timeService", interceptor, ExtendedStatisticInterceptor.class);
         replaceField(TEST_TIME_SERVICE, "timeService", lockManager, ExtendedStatisticLockManager.class);
         replaceField(TEST_TIME_SERVICE, "timeService", rpcManager, ExtendedStatisticRpcManager.class);
         controlledRpcManager[i] = new ControlledRpcManager(rpcManager);
         replaceComponent(cache(i), RpcManager.class, controlledRpcManager[i], true);
         transactionTrackInterceptors[i] = TransactionTrackInterceptor.injectInCache(cache(i));
         if (i == 0) {
            DeadlockDetectingLockManager dldLockManager = (DeadlockDetectingLockManager) lockManager.getActual();
            dldLockManager.injectTimeService(lockManagerTimeService);
         }
      }
   }

   private void assertTxSeen(int txExecutor, int localTx, int remoteTx, boolean reset) throws InterruptedException {
      for (int i = 0; i < NUM_NODES; ++i) {
         if (i == txExecutor) {
            assertTrue(transactionTrackInterceptors[i].awaitForLocalCompletion(localTx, TX_TIMEOUT, TimeUnit.SECONDS));
         } else {
            assertTrue(transactionTrackInterceptors[i].awaitForRemoteCompletion(remoteTx, TX_TIMEOUT, TimeUnit.SECONDS));
         }
         if (reset) {
            transactionTrackInterceptors[i].reset();
         }
      }
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            for (LockManager lockManager : lockManagers) {
               if (lockManager.getNumberOfLocksHeld() != 0) {
                  return false;
               }
            }
            return true;
         }
      });
      //sleepThread(1000);
   }

   private void doWriteSkewTest(boolean executeOnOwner) throws Exception {
      final int txExecutor = executeOnOwner ? 0 : 1;
      cache(0).put(KEY_1, VALUE_1);
      assertTxSeen(0, 1, 1, true);
      resetStats();
      tm(txExecutor).begin();
      assertEquals(cache(txExecutor).get(KEY_1), VALUE_1);
      Transaction tx1 = tm(txExecutor).suspend();

      tm(txExecutor).begin();
      assertEquals(cache(txExecutor).get(KEY_1), VALUE_1);
      Transaction tx2 = tm(txExecutor).suspend();

      tm(txExecutor).begin();
      cache(txExecutor).put(KEY_1, VALUE_2);
      tm(txExecutor).commit();

      tm(txExecutor).resume(tx1);
      cache(txExecutor).put(KEY_1, VALUE_3);
      try {
         tm(txExecutor).commit();
         fail("Rollback Exception expected!");
      } catch (RollbackException expected) {
         //expected
      }

      //sometimes the tx2 tries to acquire the lock and the lock was not been released by tx1 because
      // TxCompletionNotificationCommand is async. If this happens the waiting time will be != from zero and the
      // test will fail.
      boolean allLocksReleased = false;
      while (!allLocksReleased) {
         allLocksReleased = true;
         for (LockManager lockManager : lockManagers) {
            if (lockManager.getNumberOfLocksHeld() != 0) {
               allLocksReleased = false;
               TestingUtil.sleepThread(100);
               break;
            }
         }
      }

      tm(txExecutor).resume(tx2);
      cache(txExecutor).put(KEY_1, VALUE_3);
      try {
         tm(txExecutor).commit();
         fail("Rollback Exception expected!");
      } catch (RollbackException expected) {
         //expected
      }

      assertTxSeen(txExecutor, 3, executeOnOwner ? 0 : 3, true);
      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();
      assertLockingValues(statsToValidate, executeOnOwner ? 1 : 0, executeOnOwner ? 2 : 0, executeOnOwner ? 0 : 3, executeOnOwner ? 1 : 0, executeOnOwner ? 2 : 0, executeOnOwner ? 0 : 1, executeOnOwner ? 0 : 2, 0, 0, executeOnOwner);
      assertWriteSkewValues(statsToValidate, 2, 3, txExecutor);
      assertAllStatsValidated(statsToValidate);
      resetStats();
   }

   private void doTimeoutTest(boolean executeOnOwner, boolean remoteContention) throws Exception {
      final int txExecutor = executeOnOwner ? 0 : 1;
      final int successTxExecutor = remoteContention ? 1 : 0;
      cache(successTxExecutor).put(KEY_1, VALUE_1);
      assertTxSeen(successTxExecutor, 1, 1, true);
      resetStats();

      tm(successTxExecutor).begin();
      cache(successTxExecutor).put(KEY_1, VALUE_2);
      final Transaction transaction = tm(successTxExecutor).suspend();

      controlledRpcManager[successTxExecutor].blockBefore(CommitCommand.class, VersionedCommitCommand.class);

      Future future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            tm(successTxExecutor).resume(transaction);
            tm(successTxExecutor).commit();
            return null;
         }
      });

      controlledRpcManager[successTxExecutor].waitForCommandToBlock();

      lockManagerTimeService.triggerTimeout = true;

      tm(txExecutor).begin();
      cache(txExecutor).put(KEY_1, VALUE_3);
      try {
         tm(txExecutor).commit();
         fail("Rollback Exception expected!");
      } catch (RollbackException expected) {
         //expected timeout exception
      }

      tm(txExecutor).begin();
      cache(txExecutor).put(KEY_1, VALUE_3);
      try {
         tm(txExecutor).commit();
         fail("Rollback Exception expected!");
      } catch (RollbackException expected) {
         //expected timeout exception
      }

      controlledRpcManager[successTxExecutor].stopBlocking();
      future.get();

      if (txExecutor == successTxExecutor) {
         assertTxSeen(successTxExecutor, 3, executeOnOwner ? 0 : 3, true);
      } else {
         assertTxSeen(successTxExecutor, 1, 1, false);
         assertTxSeen(txExecutor, 2, executeOnOwner ? 0 : 2, true);
      }
      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();
      assertLockingValues(statsToValidate, remoteContention ? 0 : 1, 0, remoteContention ? 1 : 0, remoteContention ? 0 : 1, executeOnOwner ? 2 : 0, remoteContention ? 1 : 0, executeOnOwner ? 0 : 2, remoteContention ? 0 : 2, remoteContention ? 2 : 0, executeOnOwner);
      assertWriteSkewValues(statsToValidate, 0, 0, txExecutor);
      assertAllStatsValidated(statsToValidate);
      resetStats();
   }

   private EnumSet<ExtendedStatistic> getStatsToValidate() {
      return EnumSet.of(LOCK_HOLD_TIME_LOCAL, LOCK_HOLD_TIME_REMOTE, NUM_LOCK_PER_LOCAL_TX, NUM_LOCK_PER_REMOTE_TX,
                        NUM_HELD_LOCKS_SUCCESS_LOCAL_TX, LOCK_HOLD_TIME_SUCCESS_LOCAL_TX, LOCK_HOLD_TIME,
                        NUM_HELD_LOCKS, NUM_WAITED_FOR_LOCKS, LOCK_WAITING_TIME, NUM_LOCK_FAILED_TIMEOUT,
                        NUM_LOCK_FAILED_DEADLOCK, NUM_WRITE_SKEW, WRITE_SKEW_PROBABILITY);
   }

   private void assertAllStatsValidated(EnumSet<ExtendedStatistic> statsToValidate) {
      assertTrue(statsToValidate.isEmpty(), "Stats not validated: " + statsToValidate + ".");
   }

   private void resetStats() {
      for (ExtendedStatisticInterceptor interceptor : extendedStatisticInterceptors) {
         interceptor.resetStatistics();
         for (ExtendedStatistic extendedStatistic : values()) {
            assertEquals(interceptor.getAttribute(extendedStatistic), 0.0, "Attribute " + extendedStatistic +
                  " is not zero after reset");
         }
      }
      for (TransactionTrackInterceptor interceptor : transactionTrackInterceptors) {
         interceptor.reset();
      }
   }

   private void assertLockingValue(ExtendedStatistic attr, EnumSet<ExtendedStatistic> statsToValidate,
                                   double lockOwnerValue, double nonLockOwnerValue) {
      assertTrue(statsToValidate.contains(attr), "Attribute " + attr + " already validated");
      for (int i = 0; i < NUM_NODES; ++i) {
         assertEquals(extendedStatisticInterceptors[i].getAttribute(attr), i == 0 ? lockOwnerValue : nonLockOwnerValue,
                      "Attribute " + attr + " has wrong value for cache " + i + ".");
      }
      statsToValidate.remove(attr);
   }

   private void assertAttributeValue(ExtendedStatistic attr, EnumSet<ExtendedStatistic> statsToValidate,
                                     double txExecutorValue, double nonTxExecutorValue, int txExecutorIndex) {
      assertTrue(statsToValidate.contains(attr), "Attribute " + attr + " already validated");
      for (int i = 0; i < NUM_NODES; ++i) {
         assertEquals(extendedStatisticInterceptors[i].getAttribute(attr), i == txExecutorIndex ? txExecutorValue : nonTxExecutorValue,
                      "Attribute " + attr + " has wrong value for cache " + i + ".");
      }
      statsToValidate.remove(attr);
   }

   private void assertLockingValues(EnumSet<ExtendedStatistic> statsToValidate, int localHeldLocks, int failLocalHeldLocks, int remoteHeldLocks,
                                    int successLocalTx, int failLocalTx, int successRemoteTx, int failRemoteTx, int timeoutLocalLocks, int timeoutRemoteLocks, boolean executeOnLockOnwer) {
      log.infof("Check Locking value. localHeldLocks=%s, failLocalHeldLocks=%s, remoteHeldLocks=%s, successLocalTx=%s, " +
                      "failLocalTx=%s, successRemoteTx=%s, failRemoteTx=%s, timeoutLocalLocks=%s, timeoutRemoteLocks=%s",
                localHeldLocks, failLocalHeldLocks, remoteHeldLocks, successLocalTx, failLocalTx, successRemoteTx, failRemoteTx,
                timeoutLocalLocks, timeoutRemoteLocks);
      int totalLocalLocks = localHeldLocks + failLocalHeldLocks;
      assertLockingValue(LOCK_HOLD_TIME_LOCAL, statsToValidate, totalLocalLocks != 0 ? MICROSECONDS : 0, 0);
      assertLockingValue(LOCK_HOLD_TIME_REMOTE, statsToValidate, remoteHeldLocks != 0 ? MICROSECONDS : 0, 0);
      assertLockingValue(NUM_LOCK_PER_LOCAL_TX, statsToValidate, successLocalTx != 0 || failLocalTx != 0 ? totalLocalLocks * 1.0 / (successLocalTx + failLocalTx) : 0, 0);
      assertLockingValue(NUM_LOCK_PER_REMOTE_TX, statsToValidate, successRemoteTx != 0 || failRemoteTx != 0 ? remoteHeldLocks * 1.0 / (successRemoteTx + failRemoteTx) : 0, 0);
      assertLockingValue(LOCK_HOLD_TIME_SUCCESS_LOCAL_TX, statsToValidate, 0, 0);
      assertLockingValue(NUM_HELD_LOCKS_SUCCESS_LOCAL_TX, statsToValidate, successLocalTx != 0 ? localHeldLocks * 1.0 / successLocalTx : 0, 0);
      assertLockingValue(LOCK_HOLD_TIME, statsToValidate, totalLocalLocks != 0 || remoteHeldLocks != 0 ? MICROSECONDS : 0, 0);
      assertLockingValue(NUM_HELD_LOCKS, statsToValidate, totalLocalLocks + remoteHeldLocks, 0);
      assertLockingValue(NUM_WAITED_FOR_LOCKS, statsToValidate, timeoutLocalLocks + timeoutRemoteLocks, 0);
      assertLockingValue(LOCK_WAITING_TIME, statsToValidate, timeoutLocalLocks != 0 || timeoutRemoteLocks != 0 ? MICROSECONDS : 0, 0);
      assertLockingValue(NUM_LOCK_FAILED_TIMEOUT, statsToValidate, executeOnLockOnwer ? timeoutLocalLocks + timeoutRemoteLocks : 0, executeOnLockOnwer ? 0 : timeoutLocalLocks + timeoutRemoteLocks);
      assertLockingValue(NUM_LOCK_FAILED_DEADLOCK, statsToValidate, 0, 0);
   }

   private void assertWriteSkewValues(EnumSet<ExtendedStatistic> statsToValidate, int numOfWriteSkew, int numOfTx, int txExecutor) {
      log.infof("Check Write Skew value. writeSkew=%s, writeTx=%s, txExecutor=%s", numOfWriteSkew, numOfTx, txExecutor);
      //remote puts always acquire locks
      assertAttributeValue(NUM_WRITE_SKEW, statsToValidate, numOfWriteSkew, 0, txExecutor);
      assertAttributeValue(WRITE_SKEW_PROBABILITY, statsToValidate, numOfTx != 0 ? numOfWriteSkew * 1.0 / numOfTx : 0, 0, txExecutor);
   }

   @BeforeMethod(alwaysRun = true)
   private void resetState() {
      for (ControlledRpcManager rpcManager : controlledRpcManager) {
         rpcManager.stopBlocking();
         rpcManager.stopFailing();
      }
      lockManagerTimeService.triggerTimeout = false;
   }

   private class LockManagerTimeService extends DefaultTimeService {
      private volatile boolean triggerTimeout = false;

      @Override
      public boolean isTimeExpired(long endTime) {
         return triggerTimeout;
      }
   }
}
