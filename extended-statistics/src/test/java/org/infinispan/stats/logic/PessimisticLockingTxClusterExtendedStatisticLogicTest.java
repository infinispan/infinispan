package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

import javax.transaction.SystemException;
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
@Test(groups = "functional", testName = "stats.logic.PessimisticLockingTxClusterExtendedStatisticLogicTest")
public class PessimisticLockingTxClusterExtendedStatisticLogicTest extends MultipleCacheManagersTest {

   private static final Object KEY_1 = "KEY_1";
   private static final Object KEY_2 = "KEY_2";
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

   public void testDeadlockOnOwnerWithLocalTx() throws Exception {
      doLocalDeadlockTest(true);
   }

   @Test (groups = "unstable", description = "https://issues.jboss.org/browse/ISPN-3342")
   public void testDeadlockOnOwnerWithRemoteTx() throws Exception {
      doLocalDeadlockTest(false);
   }

   public void testDeadlockOnNonOwnerWithLocalTx() throws Exception {
      doRemoteDeadlockTest(true);
   }

   @Test (groups = "unstable", description = "https://issues.jboss.org/browse/ISPN-3342")
   public void testDeadlockOnNonOwnerWithRemoteTx() throws Exception {
      doRemoteDeadlockTest(false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ)//.writeSkewCheck(true)
               .lockAcquisitionTimeout(60000); //the timeout are triggered by the TimeService!
         builder.transaction().recovery().disable();
         builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
         builder.deadlockDetection().enable();
         //builder.versioning().enable().scheme(VersioningScheme.SIMPLE);
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

   private void doTimeoutTest(boolean executeOnOwner, boolean remoteContention) throws Exception {
      final int txExecutor = executeOnOwner ? 0 : 1;
      final int successTxExecutor = remoteContention ? 1 : 0;
      cache(successTxExecutor).put(KEY_1, VALUE_1);
      assertTxSeen(successTxExecutor, 1, 1, true);
      resetStats();

      tm(successTxExecutor).begin();
      //lock on key 1 acquired in the lock owner.
      cache(successTxExecutor).put(KEY_1, VALUE_2);
      final Transaction transaction = tm(successTxExecutor).suspend();

      lockManagerTimeService.triggerTimeout = true;

      tm(txExecutor).begin();
      try {
         cache(txExecutor).put(KEY_1, VALUE_3);
         fail("Expected timeout exception");
      } catch (Exception expected) {
         //expected timeout exception
      } finally {
         safeRollback(txExecutor);
      }

      tm(txExecutor).begin();
      try {
         cache(txExecutor).put(KEY_1, VALUE_3);
         fail("Expected timeout exception");
      } catch (Exception expected) {
         //expected timeout exception
      } finally {
         safeRollback(txExecutor);
      }

      tm(successTxExecutor).resume(transaction);
      tm(successTxExecutor).commit();

      if (txExecutor == successTxExecutor) {
         assertTxSeen(successTxExecutor, 3, 1, true);
      } else {
         assertTxSeen(successTxExecutor, 1, 1, false);
         assertTxSeen(txExecutor, 2, 0, true);
      }
      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();
      assertLockingValues(statsToValidate, remoteContention ? 0 : 1, 0, remoteContention ? 1 : 0, remoteContention ? 0 : 1, executeOnOwner ? 2 : 0, remoteContention ? 1 : 0, 0, remoteContention ? 0 : 2, remoteContention ? 2 : 0, 0, 0, 2, executeOnOwner);
      assertWriteSkewValues(statsToValidate, 0, 0, txExecutor);
      assertAllStatsValidated(statsToValidate);
      resetStats();
   }

   private void doLocalDeadlockTest(boolean deadlockWithLocal) throws Exception {
      final int txExecutor = deadlockWithLocal ? 0 : 1;
      cache(0).put(KEY_1, VALUE_1);
      cache(0).put(KEY_2, VALUE_1);
      assertTxSeen(0, 2, 2, true);
      resetStats();

      tm(0).begin();
      //lock on key 1 acquired
      cache(0).put(KEY_1, VALUE_2);
      final Transaction tx1 = tm(txExecutor).suspend();


      tm(txExecutor).begin();
      //lock on key 2 acquired
      cache(txExecutor).put(KEY_2, VALUE_2);
      final Transaction tx2 = tm(txExecutor).suspend();


      Future<Boolean> futureTx1 = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            tm(0).resume(tx1);
            try {
               cache(0).put(KEY_2, VALUE_3);
               tm(0).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               safeRollback(0);
            }
            return Boolean.FALSE;
         }
      });

      Future<Boolean> futureTx2 = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            tm(txExecutor).resume(tx2);
            try {
               cache(txExecutor).put(KEY_1, VALUE_3);
               tm(txExecutor).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               safeRollback(txExecutor);
            }
            return Boolean.FALSE;
         }
      });

      boolean tx1Outcome = futureTx1.get();
      boolean tx2Outcome = futureTx2.get();

      assertFalse(tx1Outcome && tx2Outcome, "Deadlock expected but both transactions has been committed.");
      assertFalse(!tx1Outcome && !tx2Outcome, "Deadlock expected but both transaction has been aborted.");
      assertTrue(tx1Outcome || tx2Outcome, "Expected one transaction to be committed.");

      if (txExecutor == 0) {
         assertTxSeen(0, 2, 1, true);
      } else {
         assertTxSeen(0, 1, tx1Outcome ? 1 : 0, false);
         assertTxSeen(1, 1, tx2Outcome ? 1 : 0, true);
      }

      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();
      assertLockingValues(statsToValidate,
                          deadlockWithLocal || tx1Outcome ? 2 : 0, //local locks from committed tx
                          deadlockWithLocal || !tx1Outcome ? 1 : 0, //local locks from failed tx
                          !deadlockWithLocal ? (tx2Outcome ? 2 : 1) : 0,//remote locks (commit or failed)
                          deadlockWithLocal || tx1Outcome ? 1 : 0, //success local tx
                          deadlockWithLocal || !tx1Outcome ? 1 : 0, //failed local tx
                          !deadlockWithLocal && tx2Outcome ? 1 : 0, //success remote tx
                          0, //failed remote tx (if tx2 aborts, no rollback will be sent)
                          0, //ignored by this test
                          0, //ignored by this test
                          deadlockWithLocal || !tx1Outcome ? 1 : 0, //deadlocks
                          !deadlockWithLocal && !tx2Outcome ? 1 : 0,
                          2, //waiting for two locks, because both transaction must try to acquire the locks before the deadlock
                          true);
      assertWriteSkewValues(statsToValidate, 0, 0, txExecutor);
      assertAllStatsValidated(statsToValidate);
      resetStats();
   }

   private void doRemoteDeadlockTest(boolean deadlockWithLocal) throws Exception {
      final int txExecutor = deadlockWithLocal ? 1 : 0;
      cache(0).put(KEY_1, VALUE_1);
      cache(0).put(KEY_2, VALUE_1);
      assertTxSeen(0, 2, 2, true);
      resetStats();

      tm(1).begin();
      //lock on key 1 acquired
      cache(1).put(KEY_1, VALUE_2);
      final Transaction tx1 = tm(txExecutor).suspend();


      tm(txExecutor).begin();
      //lock on key 2 acquired
      cache(txExecutor).put(KEY_2, VALUE_2);
      final Transaction tx2 = tm(txExecutor).suspend();


      Future<Boolean> futureTx1 = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            tm(1).resume(tx1);
            try {
               cache(1).put(KEY_2, VALUE_3);
               tm(1).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               safeRollback(1);
            }
            return Boolean.FALSE;
         }
      });

      Future<Boolean> futureTx2 = fork(new Callable<Boolean>() {
         @Override
         public Boolean call() throws Exception {
            tm(txExecutor).resume(tx2);
            try {
               cache(txExecutor).put(KEY_1, VALUE_3);
               tm(txExecutor).commit();
               return Boolean.TRUE;
            } catch (Exception e) {
               safeRollback(txExecutor);
            }
            return Boolean.FALSE;
         }
      });

      boolean tx1Outcome = futureTx1.get();
      boolean tx2Outcome = futureTx2.get();

      assertFalse(tx1Outcome && tx2Outcome, "Deadlock expected but both transactions has been committed.");
      assertFalse(!tx1Outcome && !tx2Outcome, "Deadlock expected but both transaction has been aborted.");
      assertTrue(tx1Outcome || tx2Outcome, "Expected one transaction to be committed.");

      if (txExecutor == 1) {
         assertTxSeen(1, 2, 1, true);
      } else {
         assertTxSeen(1, 1, tx1Outcome ? 1 : 0, false);
         assertTxSeen(0, 1, tx2Outcome ? 1 : 0, true);
      }

      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();
      assertLockingValues(statsToValidate,
                          !deadlockWithLocal && tx2Outcome ? 2 : 0, //local locks from committed tx
                          !deadlockWithLocal && !tx2Outcome ? 1 : 0, //local locks from failed tx (the other is never acquired)
                          deadlockWithLocal ? 3 : tx1Outcome ? 2 : 1,//remote locks (commit or failed)
                          !deadlockWithLocal && tx2Outcome ? 1 : 0, //success local tx
                          !deadlockWithLocal && !tx2Outcome ? 1 : 0, //failed local tx
                          deadlockWithLocal || tx1Outcome ? 1 : 0, //success remote tx
                          0, //failed remote tx
                          0, //ignored by this test
                          0, //ignored by this test
                          !deadlockWithLocal && !tx2Outcome ? 1 : 0, //deadlocks
                          deadlockWithLocal || !tx1Outcome ? 1 : 0,
                          2, //waiting for two locks, because both transaction must try to acquire the locks before the deadlock
                          true);
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

   private void safeRollback(int index) {
      try {
         tm(index).rollback();
      } catch (SystemException e) {
         //ignored!
      }
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
                                    int successLocalTx, int failLocalTx, int successRemoteTx, int failRemoteTx, int timeoutLocalLocks, int timeoutRemoteLocks, int localDeadLock, int remoteDeadLock, int waitingForLocks, boolean executeOnLockOnwer) {
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
      assertLockingValue(NUM_WAITED_FOR_LOCKS, statsToValidate, waitingForLocks, 0);
      assertLockingValue(LOCK_WAITING_TIME, statsToValidate, waitingForLocks != 0 ? MICROSECONDS : 0, 0);
      assertLockingValue(NUM_LOCK_FAILED_TIMEOUT, statsToValidate, executeOnLockOnwer ? timeoutLocalLocks + timeoutRemoteLocks : 0, executeOnLockOnwer ? 0 : timeoutLocalLocks + timeoutRemoteLocks);
      assertLockingValue(NUM_LOCK_FAILED_DEADLOCK, statsToValidate, localDeadLock, remoteDeadLock);
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
      for (TransactionTrackInterceptor interceptor : transactionTrackInterceptors) {
         interceptor.reset();
      }
   }

   private class LockManagerTimeService extends DefaultTimeService {
      private volatile boolean triggerTimeout = false;

      @Override
      public boolean isTimeExpired(long endTime) {
         return triggerTimeout;
      }
   }
}
