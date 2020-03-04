package org.infinispan.extendedstats.logic;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.extendedstats.CacheStatisticCollector.convertNanosToMicro;
import static org.infinispan.extendedstats.container.ExtendedStatistic.LOCK_HOLD_TIME;
import static org.infinispan.extendedstats.container.ExtendedStatistic.LOCK_HOLD_TIME_LOCAL;
import static org.infinispan.extendedstats.container.ExtendedStatistic.LOCK_HOLD_TIME_REMOTE;
import static org.infinispan.extendedstats.container.ExtendedStatistic.LOCK_HOLD_TIME_SUCCESS_LOCAL_TX;
import static org.infinispan.extendedstats.container.ExtendedStatistic.LOCK_WAITING_TIME;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_HELD_LOCKS;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_HELD_LOCKS_SUCCESS_LOCAL_TX;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_LOCK_FAILED_DEADLOCK;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_LOCK_FAILED_TIMEOUT;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_LOCK_PER_LOCAL_TX;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_LOCK_PER_REMOTE_TX;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_WAITED_FOR_LOCKS;
import static org.infinispan.extendedstats.container.ExtendedStatistic.NUM_WRITE_SKEW;
import static org.infinispan.extendedstats.container.ExtendedStatistic.WRITE_SKEW_PROBABILITY;
import static org.infinispan.extendedstats.container.ExtendedStatistic.values;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractField;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.infinispan.test.TestingUtil.replaceField;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.EnumSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.extendedstats.CacheStatisticCollector;
import org.infinispan.extendedstats.CacheStatisticManager;
import org.infinispan.extendedstats.container.ConcurrentGlobalContainer;
import org.infinispan.extendedstats.container.ExtendedStatistic;
import org.infinispan.extendedstats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.extendedstats.wrappers.ExtendedStatisticLockManager;
import org.infinispan.extendedstats.wrappers.ExtendedStatisticRpcManager;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.ReplicatedControlledConsistentHashFactory;
import org.infinispan.util.TransactionTrackInterceptor;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "extendedstats.logic.OptimisticLockingTxClusterExtendedStatisticLogicTest")
public class OptimisticLockingTxClusterExtendedStatisticLogicTest extends MultipleCacheManagersTest {

   private static final Object KEY_1 = "KEY_1";
   private static final Object VALUE_1 = "VALUE_1";
   private static final Object VALUE_2 = "VALUE_2";
   private static final Object VALUE_3 = "VALUE_3";
   private static final int NUM_NODES = 2;
   private static final int TX_TIMEOUT = 60;
   private static final TimeService TEST_TIME_SERVICE = new EmbeddedTimeService() {
      @Override
      public long time() {
         return 0;
      }

      @Override
      public long timeDuration(long startTimeNanos, TimeUnit outputTimeUnit) {
         assertEquals(startTimeNanos, 0, "Start timestamp must be zero!");
         assertEquals(outputTimeUnit, NANOSECONDS, "TimeUnit is different from expected");
         return 1;
      }

      @Override
      public long timeDuration(long startTimeNanos, long endTimeNanos, TimeUnit outputTimeUnit) {
         assertEquals(startTimeNanos, 0, "Start timestamp must be zero!");
         assertEquals(endTimeNanos, 0, "End timestamp must be zero!");
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

   public void testWriteSkewOnNonOwner() throws Exception {
      doWriteSkewTest(false);
   }

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
         builder.clustering().hash().numSegments(1)
               .consistentHashFactory(new ReplicatedControlledConsistentHashFactory(0));
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
               .lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
         builder.transaction().recovery().disable();
         builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
         extendedStatisticInterceptors[i] = new ExtendedStatisticInterceptor();
         builder.customInterceptors().addInterceptor().interceptor(extendedStatisticInterceptors[i])
               .after(TxInterceptor.class);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
      for (int i = 0; i < NUM_NODES; ++i) {
         ExtendedStatisticInterceptor interceptor = extendedStatisticInterceptors[i];
         CacheStatisticManager manager = extractField(interceptor, "cacheStatisticManager");
         CacheStatisticCollector collector = extractField(manager, "cacheStatisticCollector");
         ConcurrentGlobalContainer globalContainer = extractField(collector, "globalContainer");
         ExtendedStatisticRpcManager rpcManager = (ExtendedStatisticRpcManager) extractComponent(cache(i), RpcManager.class);
         ExtendedStatisticLockManager lockManager = (ExtendedStatisticLockManager) extractLockManager(cache(i));
         lockManagers[i] = lockManager;
         replaceField(TEST_TIME_SERVICE, "timeService", manager, CacheStatisticManager.class);
         replaceField(TEST_TIME_SERVICE, "timeService", collector, CacheStatisticCollector.class);
         replaceField(TEST_TIME_SERVICE, "timeService", globalContainer, ConcurrentGlobalContainer.class);
         replaceField(TEST_TIME_SERVICE, "timeService", interceptor, ExtendedStatisticInterceptor.class);
         replaceField(TEST_TIME_SERVICE, "timeService", lockManager, ExtendedStatisticLockManager.class);
         replaceField(TEST_TIME_SERVICE, "timeService", rpcManager, ExtendedStatisticRpcManager.class);
         controlledRpcManager[i] = ControlledRpcManager.replaceRpcManager(cache(i));
         transactionTrackInterceptors[i] = TransactionTrackInterceptor.injectInCache(cache(i));
         if (i == 0) {
            LockManager actualLockManager = lockManager.getActual();
            LockContainer container = extractField(actualLockManager, "lockContainer");
            TestingUtil.inject(container, new WithinThreadExecutor(), lockManagerTimeService);
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
      assertNoLocks();
   }

   private void assertNoLocks() {
      for (LockManager lockManager : lockManagers) {
         eventuallyEquals(0, lockManager::getNumberOfLocksHeld);
      }
   }

   private void doWriteSkewTest(boolean executeOnOwner) throws Exception {
      controlledRpcManager[0].excludeCommands(PrepareCommand.class, CommitCommand.class, RollbackCommand.class, TxCompletionNotificationCommand.class);
      controlledRpcManager[1].excludeCommands(PrepareCommand.class, CommitCommand.class, RollbackCommand.class, TxCompletionNotificationCommand.class);
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

      //ensures no lock waiting time.
      assertNoLocks();

      tm(txExecutor).resume(tx1);
      cache(txExecutor).put(KEY_1, VALUE_3);
      try {
         tm(txExecutor).commit();
         fail("Rollback Exception expected!");
      } catch (RollbackException expected) {
         //expected
      }

      //ensures no lock waiting time.
      assertNoLocks();

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
      controlledRpcManager[successTxExecutor].excludeCommands(PrepareCommand.class, CommitCommand.class, TxCompletionNotificationCommand.class);
      cache(successTxExecutor).put(KEY_1, VALUE_1);
      controlledRpcManager[successTxExecutor].excludeCommands(PrepareCommand.class, TxCompletionNotificationCommand.class);
      assertTxSeen(successTxExecutor, 1, 1, true);
      resetStats();

      tm(successTxExecutor).begin();
      cache(successTxExecutor).put(KEY_1, VALUE_2);
      final Transaction transaction = tm(successTxExecutor).suspend();

      Future future = fork(() -> {
         tm(successTxExecutor).resume(transaction);
         tm(successTxExecutor).commit();
         return null;
      });

      ControlledRpcManager.BlockedRequest blockedCommit =
         controlledRpcManager[successTxExecutor].expectCommand(CommitCommand.class);

      lockManagerTimeService.triggerTimeout = true;

      controlledRpcManager[txExecutor].excludeCommands(PrepareCommand.class, RollbackCommand.class, TxCompletionNotificationCommand.class);
      tm(txExecutor).begin();
      cache(txExecutor).put(KEY_1, VALUE_3);
      Exceptions.expectException(RollbackException.class, () -> tm(txExecutor).commit());

      tm(txExecutor).begin();
      cache(txExecutor).put(KEY_1, VALUE_3);
      Exceptions.expectException(RollbackException.class, () -> tm(txExecutor).commit());

      blockedCommit.send().receiveAll();
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
   void resetState() {
      lockManagerTimeService.triggerTimeout = false;
   }

   @AfterClass
   void stopBlockingRpcs() {
      for (ControlledRpcManager rpcManager : controlledRpcManager) {
         rpcManager.stopBlocking();
      }
   }

   class LockManagerTimeService extends EmbeddedTimeService {
      private volatile boolean triggerTimeout = false;

      @Override
      public boolean isTimeExpired(long endTimeNanos) {
         return triggerTimeout;
      }
   }
}
