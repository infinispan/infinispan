package org.infinispan.stats;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.stats.wrappers.ExtendedStatisticLockManager;
import org.infinispan.stats.wrappers.ExtendedStatisticRpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.TransactionTrackInterceptor;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.CacheStatisticCollector.convertNanosToMicro;
import static org.infinispan.stats.CacheStatisticCollector.convertNanosToSeconds;
import static org.infinispan.stats.container.ExtendedStatistic.*;
import static org.infinispan.test.TestingUtil.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional")
public abstract class BaseTxClusterExtendedStatisticLogicTest extends MultipleCacheManagersTest {

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
   private static final double SECONDS = convertNanosToSeconds(TEST_TIME_SERVICE.timeDuration(0, NANOSECONDS));
   private final TransactionTrackInterceptor[] transactionTrackInterceptors = new TransactionTrackInterceptor[NUM_NODES];
   private final ExtendedStatisticInterceptor[] extendedStatisticInterceptors = new ExtendedStatisticInterceptor[NUM_NODES];
   private final LockManager[] lockManagers = new LockManager[NUM_NODES];
   private final boolean sync;
   private final boolean replicated;
   private final boolean sync2ndPhase;
   private final boolean totalOrder;
   private final CacheMode cacheMode;
   private final List<Object> keys = new ArrayList<>(128);

   protected BaseTxClusterExtendedStatisticLogicTest(CacheMode cacheMode, boolean sync2ndPhase, boolean totalOrder) {
      this.sync = cacheMode.isSynchronous();
      this.replicated = cacheMode.isReplicated();
      this.cacheMode = cacheMode;
      this.sync2ndPhase = sync2ndPhase;
      this.totalOrder = totalOrder;
   }

   public final void testPutTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.PUT, 2, 7, 3, 4, 5, false, true);
   }

   public final void testPutTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.PUT, 3, 6, 2, 5, 4, true, true);
   }

   public final void testPutTxAndReadOnlyTxNonCoordinator() throws Exception {
      testStats(WriteOperation.PUT, 4, 5, 4, 6, 3, false, false);
   }

   public final void testPutTxAndReadOnlyTxRollbackNonCoordinator() throws Exception {
      testStats(WriteOperation.PUT, 5, 4, 5, 7, 2, true, false);
   }

   public final void testConditionalPutTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.PUT_IF, 2, 7, 3, 4, 5, false, true);
   }

   public final void testConditionalPutTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.PUT_IF, 3, 6, 2, 5, 4, true, true);
   }

   public final void testConditionalPutTxAndReadOnlyTxNonCoordinator() throws Exception {
      testStats(WriteOperation.PUT_IF, 4, 5, 4, 6, 3, false, false);
   }

   public final void testConditionalPutTxAndReadOnlyTxRollbackNonCoordinator() throws Exception {
      testStats(WriteOperation.PUT_IF, 5, 4, 5, 7, 2, true, false);
   }

   public final void testReplaceTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REPLACE, 2, 7, 3, 4, 5, false, true);
   }

   public final void testReplaceTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REPLACE, 3, 6, 2, 5, 4, true, true);
   }

   public final void testReplaceTxAndReadOnlyTxNonCoordinator() throws Exception {
      testStats(WriteOperation.REPLACE, 4, 5, 4, 6, 3, false, false);
   }

   public final void testReplaceTxAndReadOnlyTxRollbackNonCoordinator() throws Exception {
      testStats(WriteOperation.REPLACE, 5, 4, 5, 7, 2, true, false);
   }

   public final void testConditionalReplaceTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REPLACE_IF, 2, 7, 3, 4, 5, false, true);
   }

   public final void testConditionalReplaceTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REPLACE_IF, 3, 6, 2, 5, 4, true, true);
   }

   public final void testConditionalReplaceTxAndReadOnlyTxNonCoordinator() throws Exception {
      testStats(WriteOperation.REPLACE_IF, 4, 5, 4, 6, 3, false, false);
   }

   public final void testConditionalReplaceTxAndReadOnlyTxRollbackNonCoordinator() throws Exception {
      testStats(WriteOperation.REPLACE_IF, 5, 4, 5, 7, 2, true, false);
   }

   public final void testRemoveTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REMOVE, 2, 7, 3, 4, 5, false, true);
   }

   public final void testRemoveTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REMOVE, 3, 6, 2, 5, 4, true, true);
   }

   public final void testRemoveTxAndReadOnlyTxNonCoordinator() throws Exception {
      testStats(WriteOperation.REMOVE, 4, 5, 4, 6, 3, false, false);
   }

   public final void testRemoveTxAndReadOnlyTxRollbackNonCoordinator() throws Exception {
      testStats(WriteOperation.REMOVE, 5, 4, 5, 7, 2, true, false);
   }

   public final void testConditionalRemoveTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REMOVE_IF, 2, 7, 3, 4, 5, false, true);
   }

   public final void testConditionalRemoveTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REMOVE_IF, 3, 6, 2, 5, 4, true, true);
   }

   public final void testConditionalRemoveTxAndReadOnlyTxNonCoordinator() throws Exception {
      testStats(WriteOperation.REMOVE_IF, 4, 5, 4, 6, 3, false, false);
   }

   public final void testConditionalRemoveTxAndReadOnlyTxRollbackNonCoordinator() throws Exception {
      testStats(WriteOperation.REMOVE_IF, 5, 4, 5, 7, 2, true, false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; ++i) {
         ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
         builder.transaction().syncCommitPhase(sync2ndPhase).syncRollbackPhase(sync2ndPhase);
         if (totalOrder) {
            builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
         }
         builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false)
               .lockAcquisitionTimeout(0);
         builder.clustering().hash().numOwners(1);
         builder.transaction().recovery().disable();
         extendedStatisticInterceptors[i] = new ExtendedStatisticInterceptor();
         builder.customInterceptors().addInterceptor().interceptor(extendedStatisticInterceptors[i])
               .after(TxInterceptor.class);
         addClusterEnabledCacheManager(builder);
      }
      waitForClusterToForm();
      for (int i = 0; i < NUM_NODES; ++i) {
         lockManagers[i] = extractLockManager(cache(i));
         ExtendedStatisticInterceptor interceptor = extendedStatisticInterceptors[i];
         CacheStatisticManager manager = extractField(interceptor, "cacheStatisticManager");
         CacheStatisticCollector collector = extractField(manager, "cacheStatisticCollector");
         ConcurrentGlobalContainer globalContainer = extractField(collector, "globalContainer");
         replaceField(TEST_TIME_SERVICE, "timeService", manager, CacheStatisticManager.class);
         replaceField(TEST_TIME_SERVICE, "timeService", collector, CacheStatisticCollector.class);
         replaceField(TEST_TIME_SERVICE, "timeService", globalContainer, ConcurrentGlobalContainer.class);
         replaceField(TEST_TIME_SERVICE, "timeService", interceptor, ExtendedStatisticInterceptor.class);
         replaceField(TEST_TIME_SERVICE, "timeService", lockManagers[i], ExtendedStatisticLockManager.class);
         replaceField(TEST_TIME_SERVICE, "timeService", extractComponent(cache(i), RpcManager.class), ExtendedStatisticRpcManager.class);
         transactionTrackInterceptors[i] = TransactionTrackInterceptor.injectInCache(cache(i));
      }
   }

   private void testStats(WriteOperation operation, int numOfWriteTx, int numOfWrites, int numOfReadsPerWriteTx,
                          int numOfReadOnlyTx, int numOfReadPerReadTx, boolean abort, boolean executeOnCoordinator)
         throws Exception {
      final int txExecutor = executeOnCoordinator ? 0 : 1;

      int localGetsReadTx = 0;
      int localGetsWriteTx = 0;
      int localPuts = 0;
      int remoteGetsReadTx = 0;
      int remoteGetsWriteTx = 0;
      int remotePuts = 0;
      int localLocks = 0;
      int remoteLocks = 0;
      int numOfLocalWriteTx = 0;
      int numOfRemoteWriteTx = 0; //no. transaction that involves the second node too.

      resetTxCounters();
      boolean remote = false;
      tm(txExecutor).begin();
      for (int i = 1; i <= (numOfReadsPerWriteTx + numOfWrites) * numOfWriteTx + numOfReadPerReadTx * numOfReadOnlyTx; ++i) {
         cache(txExecutor).put(getKey(i), getInitValue(i));
         if (isRemote(getKey(i), cache(txExecutor))) {
            remote = true;
         }
      }
      tm(txExecutor).commit();
      assertTxSeen(txExecutor, 1, replicated || remote ? 1 : 0, 0, false);
      resetStats();
      resetTxCounters();

      int keyIndex = 0;
      //write tx
      for (int tx = 1; tx <= numOfWriteTx; ++tx) {
         boolean involvesRemoteNode = cacheMode.isReplicated();
         tm(txExecutor).begin();
         for (int i = 1; i <= numOfReadsPerWriteTx; ++i) {
            keyIndex++;
            Object key = getKey(keyIndex);
            if (isRemote(key, cache(txExecutor))) {
               remoteGetsWriteTx++;
            } else {
               localGetsWriteTx++;
            }
            assertEquals(cache(txExecutor).get(key), getInitValue(keyIndex));
         }
         for (int i = 1; i <= numOfWrites; ++i) {
            keyIndex++;
            Object key = operation == WriteOperation.PUT_IF ? getKey(-keyIndex) : getKey(keyIndex);
            switch (operation) {
               case PUT:
                  cache(txExecutor).put(key, getValue(keyIndex));
                  break;
               case PUT_IF:
                  cache(txExecutor).putIfAbsent(key, getValue(keyIndex));
                  break;
               case REPLACE:
                  cache(txExecutor).replace(key, getValue(keyIndex));
                  break;
               case REPLACE_IF:
                  cache(txExecutor).replace(key, getInitValue(keyIndex), getValue(keyIndex));
                  break;
               case REMOVE:
                  cache(txExecutor).remove(key);
                  break;
               case REMOVE_IF:
                  cache(txExecutor).remove(key, getInitValue(keyIndex));
                  break;
               default:
                  //nothing
            }
            if (isRemote(key, cache(txExecutor))) {
               remotePuts++;
               involvesRemoteNode = true;
            } else {
               localPuts++;
            }
            if (isLockOwner(key, cache(txExecutor))) {
               if (!abort) {
                  localLocks++;
               }
            } else {
               if (!abort) {
                  remoteLocks++;
               }
            }

         }
         if (involvesRemoteNode) {
            numOfRemoteWriteTx++;
         }
         numOfLocalWriteTx++;
         if (abort) {
            tm(txExecutor).rollback();
         } else {
            tm(txExecutor).commit();

         }
      }

      //read tx
      for (int tx = 1; tx <= numOfReadOnlyTx; ++tx) {
         tm(txExecutor).begin();
         for (int i = 1; i <= numOfReadPerReadTx; ++i) {
            keyIndex++;
            Object key = getKey(keyIndex);
            if (isRemote(key, cache(txExecutor))) {
               remoteGetsReadTx++;
            } else {
               localGetsReadTx++;
            }
            assertEquals(cache(txExecutor).get(key), getInitValue(keyIndex));
         }
         if (abort) {
            tm(txExecutor).rollback();
         } else {
            tm(txExecutor).commit();

         }
      }
      assertTxSeen(txExecutor, numOfLocalWriteTx, replicated ? numOfLocalWriteTx : numOfRemoteWriteTx, numOfReadOnlyTx, abort);

      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();

      assertTxValues(statsToValidate, numOfLocalWriteTx, numOfRemoteWriteTx, numOfReadOnlyTx, txExecutor, abort);
      assertLockingValues(statsToValidate, localLocks, remoteLocks, numOfLocalWriteTx, numOfRemoteWriteTx, txExecutor, abort);
      assertAccessesValues(statsToValidate, localGetsReadTx, remoteGetsReadTx, localGetsWriteTx, remoteGetsWriteTx,
                           localPuts, remotePuts, numOfWriteTx, numOfReadOnlyTx, txExecutor, abort);

      assertAttributeValue(NUM_WRITE_SKEW, statsToValidate, 0, 0, txExecutor);
      assertAttributeValue(WRITE_SKEW_PROBABILITY, statsToValidate, 0, 0, txExecutor);

      assertAllStatsValidated(statsToValidate);
      resetStats();
   }

   private void assertTxSeen(int txExecutor, int localTx, int remoteTx, int readOnlyTx, boolean abort) throws InterruptedException {
      for (int i = 0; i < NUM_NODES; ++i) {
         if (i == txExecutor) {
            assertTrue(transactionTrackInterceptors[i].awaitForLocalCompletion(localTx + readOnlyTx, TX_TIMEOUT, TimeUnit.SECONDS));
            if (totalOrder && !abort) {
               assertTrue(transactionTrackInterceptors[i].awaitForRemoteCompletion(localTx, TX_TIMEOUT, TimeUnit.SECONDS));
            }
         } else if (!abort) {
            assertTrue(transactionTrackInterceptors[i].awaitForRemoteCompletion(remoteTx, TX_TIMEOUT, TimeUnit.SECONDS));
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
   }

   private void resetTxCounters() {
      for (TransactionTrackInterceptor interceptor : transactionTrackInterceptors) {
         interceptor.reset();
      }
   }

   private boolean isRemote(Object key, Cache cache) {
      return !replicated && !DistributionTestHelper.isOwner(cache, key);
   }

   private boolean isLockOwner(Object key, Cache cache) {
      return replicated ? address(0).equals(address(cache)) : DistributionTestHelper.isFirstOwner(cache, key);
   }

   private Object getKey(int i) {
      if (i < 0) {
         return replicated ? "KEY_" + i : new MagicKey("KEY_" + i, cache(0));
      }
      for (int j = keys.size(); j < i; ++j) {
         keys.add(replicated ? "KEY_" + (j + 1) : new MagicKey("KEY_" + (j + 1), cache(0)));
      }
      return keys.get(i - 1);
   }

   private Object getInitValue(int i) {
      return "INIT_" + i;
   }

   private Object getValue(int i) {
      return "VALUE_" + i;
   }

   private void assertTxValues(EnumSet<ExtendedStatistic> statsToValidate, int numOfLocalWriteTx, int numOfRemoteWriteTx,
                               int numOfReadTx, int txExecutor, boolean abort) {
      log.infof("Check Tx value: localWriteTx=%s, remoteWriteTx=%s, readTx=%s, txExecutor=%s, abort?=%s",
                numOfLocalWriteTx, numOfRemoteWriteTx, numOfReadTx, txExecutor, abort);
      if (abort) {
         assertAttributeValue(NUM_COMMITTED_RO_TX, statsToValidate, 0, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_WR_TX, statsToValidate, 0, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_WR_TX, statsToValidate, numOfLocalWriteTx, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_RO_TX, statsToValidate, numOfReadTx, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_LOCAL_COMMITTED_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCAL_EXEC_NO_CONT, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(WRITE_TX_PERCENTAGE, statsToValidate, numOfLocalWriteTx * 1.0 / (numOfLocalWriteTx + numOfReadTx), 0, txExecutor);
         assertAttributeValue(SUCCESSFUL_WRITE_TX_PERCENTAGE, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(WR_TX_ABORTED_EXECUTION_TIME, statsToValidate, numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(RO_TX_ABORTED_EXECUTION_TIME, statsToValidate, numOfReadTx, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ABORT_RATE, statsToValidate, 1, 0, txExecutor);
         assertAttributeValue(ARRIVAL_RATE, statsToValidate, (numOfLocalWriteTx + numOfReadTx) / SECONDS, 0, txExecutor);
         assertAttributeValue(THROUGHPUT, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ROLLBACK_EXECUTION_TIME, statsToValidate, numOfReadTx != 0 || numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(NUM_ROLLBACK_COMMAND, statsToValidate, numOfReadTx + numOfLocalWriteTx, 0, txExecutor);
         assertAttributeValue(LOCAL_ROLLBACK_EXECUTION_TIME, statsToValidate, numOfReadTx != 0 || numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(REMOTE_ROLLBACK_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(COMMIT_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_COMMIT_COMMAND, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCAL_COMMIT_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(REMOTE_COMMIT_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(PREPARE_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor); // //not exposed via JMX
         assertAttributeValue(NUM_PREPARE_COMMAND, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCAL_PREPARE_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(REMOTE_PREPARE_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_SYNC_PREPARE, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(SYNC_PREPARE_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_SYNC_COMMIT, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(SYNC_COMMIT_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_SYNC_ROLLBACK, statsToValidate, sync2ndPhase && !totalOrder ? numOfLocalWriteTx : 0, 0, txExecutor);
         assertAttributeValue(SYNC_ROLLBACK_TIME, statsToValidate, sync2ndPhase && numOfLocalWriteTx != 0 && !totalOrder ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(ASYNC_PREPARE_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_PREPARE, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ASYNC_COMMIT_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_COMMIT, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ASYNC_ROLLBACK_TIME, statsToValidate, (sync && sync2ndPhase) || numOfLocalWriteTx == 0 || totalOrder ? 0 : MICROSECONDS, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_ROLLBACK, statsToValidate, (sync && sync2ndPhase) || totalOrder ? 0 : numOfLocalWriteTx, 0, txExecutor);
         assertAttributeValue(ASYNC_COMPLETE_NOTIFY_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_COMPLETE_NOTIFY, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_PREPARE, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_COMMIT, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_ROLLBACK, statsToValidate, !totalOrder && numOfLocalWriteTx != 0 && replicated ? NUM_NODES : 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_COMPLETE_NOTIFY, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(RESPONSE_TIME, statsToValidate, 0, 0, txExecutor);
      } else {
         assertAttributeValue(NUM_COMMITTED_RO_TX, statsToValidate, numOfReadTx, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_WR_TX, statsToValidate, (totalOrder ? 2 * numOfLocalWriteTx : numOfLocalWriteTx),
                              numOfRemoteWriteTx, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_WR_TX, statsToValidate, 0, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_RO_TX, statsToValidate, 0, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_TX, statsToValidate, (totalOrder ? 2 * numOfLocalWriteTx : numOfLocalWriteTx) + numOfReadTx,
                              numOfRemoteWriteTx, txExecutor);
         assertAttributeValue(NUM_LOCAL_COMMITTED_TX, statsToValidate, numOfReadTx + numOfLocalWriteTx, 0, txExecutor);
         assertAttributeValue(LOCAL_EXEC_NO_CONT, statsToValidate, numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(WRITE_TX_PERCENTAGE, statsToValidate, (numOfLocalWriteTx * 1.0) / (numOfReadTx + numOfLocalWriteTx), 0, txExecutor);
         assertAttributeValue(SUCCESSFUL_WRITE_TX_PERCENTAGE, statsToValidate, (numOfReadTx + numOfLocalWriteTx) > 0 ? (numOfLocalWriteTx * 1.0) / (numOfReadTx + numOfLocalWriteTx) : 0, 0, txExecutor);
         assertAttributeValue(WR_TX_ABORTED_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(RO_TX_ABORTED_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor); //not exposed via JMX
         assertAttributeValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, numOfReadTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(ABORT_RATE, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ARRIVAL_RATE, statsToValidate, ((totalOrder ? 2 * numOfLocalWriteTx : numOfLocalWriteTx) + numOfReadTx) / SECONDS, numOfRemoteWriteTx / SECONDS, txExecutor);
         assertAttributeValue(THROUGHPUT, statsToValidate, (numOfLocalWriteTx + numOfReadTx) / SECONDS, 0, txExecutor);
         assertAttributeValue(ROLLBACK_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_ROLLBACK_COMMAND, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCAL_ROLLBACK_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(REMOTE_ROLLBACK_EXECUTION_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(COMMIT_EXECUTION_TIME, statsToValidate, sync && (numOfReadTx != 0 || numOfLocalWriteTx != 0) && !totalOrder ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(NUM_COMMIT_COMMAND, statsToValidate, sync && !totalOrder ? (numOfReadTx + numOfLocalWriteTx) : 0, sync && !totalOrder ? numOfRemoteWriteTx : 0, txExecutor);
         assertAttributeValue(LOCAL_COMMIT_EXECUTION_TIME, statsToValidate, sync && (numOfReadTx != 0 || numOfLocalWriteTx != 0) && !totalOrder ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(REMOTE_COMMIT_EXECUTION_TIME, statsToValidate, 0, sync && numOfRemoteWriteTx != 0 && !totalOrder ? MICROSECONDS : 0, txExecutor);
         assertAttributeValue(PREPARE_EXECUTION_TIME, statsToValidate, numOfReadTx + (totalOrder ? 2 * numOfLocalWriteTx : numOfLocalWriteTx), numOfRemoteWriteTx, txExecutor); // //not exposed via JMX
         assertAttributeValue(NUM_PREPARE_COMMAND, statsToValidate, numOfReadTx + (totalOrder ? 2 * numOfLocalWriteTx : numOfLocalWriteTx), numOfRemoteWriteTx, txExecutor);
         assertAttributeValue(LOCAL_PREPARE_EXECUTION_TIME, statsToValidate, (numOfReadTx != 0 || numOfLocalWriteTx != 0) ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(REMOTE_PREPARE_EXECUTION_TIME, statsToValidate, totalOrder && numOfLocalWriteTx != 0 ? MICROSECONDS : 0, numOfRemoteWriteTx != 0 ? MICROSECONDS : 0, txExecutor);
         assertAttributeValue(NUM_SYNC_PREPARE, statsToValidate, sync ? numOfLocalWriteTx : 0, 0, txExecutor);
         assertAttributeValue(SYNC_PREPARE_TIME, statsToValidate, sync && numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(NUM_SYNC_COMMIT, statsToValidate, sync2ndPhase && !totalOrder ? numOfLocalWriteTx : 0, 0, txExecutor);
         assertAttributeValue(SYNC_COMMIT_TIME, statsToValidate, sync2ndPhase && numOfLocalWriteTx != 0 && !totalOrder ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(NUM_SYNC_ROLLBACK, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(SYNC_ROLLBACK_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ASYNC_PREPARE_TIME, statsToValidate, !sync && numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_PREPARE, statsToValidate, sync ? 0 : numOfLocalWriteTx, 0, txExecutor);
         assertAttributeValue(ASYNC_COMMIT_TIME, statsToValidate, (!sync || sync2ndPhase) || numOfLocalWriteTx == 0 || totalOrder ? 0 : MICROSECONDS, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_COMMIT, statsToValidate, (!sync || sync2ndPhase) || totalOrder ? 0 : numOfLocalWriteTx, 0, txExecutor);
         assertAttributeValue(ASYNC_ROLLBACK_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_ROLLBACK, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(ASYNC_COMPLETE_NOTIFY_TIME, statsToValidate, sync2ndPhase && numOfLocalWriteTx != 0 && !totalOrder ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(NUM_ASYNC_COMPLETE_NOTIFY, statsToValidate, sync2ndPhase && !totalOrder ? numOfLocalWriteTx : 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_PREPARE, statsToValidate, replicated ? NUM_NODES : totalOrder && txExecutor == 1 ? 2 : 1, 0, txExecutor);
         assertAttributeValue(NUM_NODES_COMMIT, statsToValidate, sync && !totalOrder ? (replicated ? NUM_NODES : 1) : 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_ROLLBACK, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_NODES_COMPLETE_NOTIFY, statsToValidate, sync2ndPhase && !totalOrder ? (replicated ? NUM_NODES : 1) : 0, 0, txExecutor);
         assertAttributeValue(RESPONSE_TIME, statsToValidate, numOfReadTx != 0 || numOfLocalWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
      }
   }

   private void assertLockingValues(EnumSet<ExtendedStatistic> statsToValidate, int numOfLocalLocks, int numOfRemoteLocks,
                                    int numOfLocalWriteTx, int numOfRemoteWriteTx, int txExecutor, boolean abort) {
      log.infof("Check Locking value. localLocks=%s, remoteLocks=%s, localWriteTx=%s, remoteWriteTx=%s, txExecutor=%s, abort?=%s",
                numOfLocalLocks, numOfRemoteLocks, numOfLocalWriteTx, numOfRemoteWriteTx, txExecutor, abort);
      if (totalOrder) {
         assertAttributeValue(LOCK_HOLD_TIME_LOCAL, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCK_HOLD_TIME_REMOTE, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_LOCK_PER_LOCAL_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_LOCK_PER_REMOTE_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_HELD_LOCKS_SUCCESS_LOCAL_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCK_HOLD_TIME_SUCCESS_LOCAL_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCK_HOLD_TIME, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(NUM_HELD_LOCKS, statsToValidate, 0, 0, txExecutor);
      } else {
         //remote puts always acquire locks
         assertAttributeValue(LOCK_HOLD_TIME_LOCAL, statsToValidate, numOfLocalLocks != 0 ? MICROSECONDS : 0, 0, txExecutor);
         assertAttributeValue(LOCK_HOLD_TIME_REMOTE, statsToValidate, 0, numOfRemoteLocks != 0 ? MICROSECONDS : 0, txExecutor);
         assertAttributeValue(NUM_LOCK_PER_LOCAL_TX, statsToValidate, numOfLocalWriteTx != 0 ? numOfLocalLocks * 1.0 / numOfLocalWriteTx : 0, 0, txExecutor);
         assertAttributeValue(NUM_LOCK_PER_REMOTE_TX, statsToValidate, 0, numOfRemoteWriteTx != 0 ? numOfRemoteLocks * 1.0 / numOfRemoteWriteTx : 0, txExecutor);
         assertAttributeValue(NUM_HELD_LOCKS_SUCCESS_LOCAL_TX, statsToValidate, !abort && numOfLocalWriteTx != 0 ? numOfLocalLocks * 1.0 / numOfLocalWriteTx : 0, 0, txExecutor);
         assertAttributeValue(LOCK_HOLD_TIME_SUCCESS_LOCAL_TX, statsToValidate, 0, 0, txExecutor);
         assertAttributeValue(LOCK_HOLD_TIME, statsToValidate, numOfLocalLocks != 0 ? MICROSECONDS : 0, numOfRemoteLocks != 0 ? MICROSECONDS : 0, txExecutor);
         assertAttributeValue(NUM_HELD_LOCKS, statsToValidate, numOfLocalLocks, numOfRemoteLocks, txExecutor);
      }
      assertAttributeValue(NUM_WAITED_FOR_LOCKS, statsToValidate, 0, 0, txExecutor);
      assertAttributeValue(LOCK_WAITING_TIME, statsToValidate, 0, 0, txExecutor);
      assertAttributeValue(NUM_LOCK_FAILED_TIMEOUT, statsToValidate, 0, 0, txExecutor);
      assertAttributeValue(NUM_LOCK_FAILED_DEADLOCK, statsToValidate, 0, 0, txExecutor);
   }

   private void assertAccessesValues(EnumSet<ExtendedStatistic> statsToValidate, int localGetsReadTx, int remoteGetsReadTx, int localGetsWriteTx, int remoteGetsWriteTx, int localPuts,
                                     int remotePuts, int numOfWriteTx, int numOfReadTx, int txExecutor, boolean abort) {
      log.infof("Check accesses values. localGetsReadTx=%s, remoteGetsReadTx=%s, localGetsWriteTx=%s, remoteGetsWriteTx=%s, " +
                      "localPuts=%s, remotePuts=%s, writeTx=%s, readTx=%s, txExecutor=%s, abort?=%s",
                localGetsReadTx, remoteGetsReadTx, localGetsWriteTx, remoteGetsWriteTx, localPuts, remotePuts,
                numOfWriteTx, numOfReadTx, txExecutor, abort);
      assertAttributeValue(NUM_REMOTE_PUT, statsToValidate, remotePuts, 0, txExecutor);
      assertAttributeValue(LOCAL_PUT_EXECUTION, statsToValidate, 0, 0, txExecutor);
      assertAttributeValue(REMOTE_PUT_EXECUTION, statsToValidate, remotePuts != 0 ? MICROSECONDS : 0, 0, txExecutor);
      assertAttributeValue(NUM_PUT, statsToValidate, remotePuts + localPuts, 0, txExecutor);
      assertAttributeValue(NUM_PUTS_WR_TX, statsToValidate, !abort && numOfWriteTx != 0 ? (localPuts + remotePuts * 1.0) / numOfWriteTx : 0, 0, txExecutor);
      assertAttributeValue(NUM_REMOTE_PUTS_WR_TX, statsToValidate, !abort && numOfWriteTx != 0 ? (remotePuts * 1.0) / numOfWriteTx : 0, 0, txExecutor);

      assertAttributeValue(NUM_REMOTE_GET, statsToValidate, remoteGetsReadTx + remoteGetsWriteTx, 0, txExecutor);
      assertAttributeValue(NUM_GET, statsToValidate, localGetsReadTx + localGetsWriteTx + remoteGetsReadTx + remoteGetsWriteTx, 0, txExecutor);
      assertAttributeValue(NUM_GETS_RO_TX, statsToValidate, !abort && numOfReadTx != 0 ? (localGetsReadTx + remoteGetsReadTx * 1.0) / numOfReadTx : 0, 0, txExecutor);
      assertAttributeValue(NUM_GETS_WR_TX, statsToValidate, !abort && numOfWriteTx != 0 ? (localGetsWriteTx + remoteGetsWriteTx * 1.0) / numOfWriteTx : 0, 0, txExecutor);
      assertAttributeValue(NUM_REMOTE_GETS_WR_TX, statsToValidate, !abort && numOfWriteTx != 0 ? remoteGetsWriteTx * 1.0 / numOfWriteTx : 0, 0, txExecutor);
      assertAttributeValue(NUM_REMOTE_GETS_RO_TX, statsToValidate, !abort && numOfReadTx != 0 ? remoteGetsReadTx * 1.0 / numOfReadTx : 0, 0, txExecutor);
      assertAttributeValue(ALL_GET_EXECUTION, statsToValidate, remoteGetsReadTx + localGetsReadTx + localGetsWriteTx + remoteGetsWriteTx, 0, txExecutor);
      //always zero because the all get execution and the rtt is always 1 (all get execution - rtt == 0)
      assertAttributeValue(LOCAL_GET_EXECUTION, statsToValidate, (remoteGetsReadTx + remoteGetsWriteTx) < (localGetsReadTx + localGetsWriteTx) ? MICROSECONDS : 0, 0, txExecutor);
      assertAttributeValue(REMOTE_GET_EXECUTION, statsToValidate, remoteGetsReadTx != 0 || remoteGetsWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
      assertAttributeValue(NUM_SYNC_GET, statsToValidate, remoteGetsReadTx + remoteGetsWriteTx, 0, txExecutor);
      assertAttributeValue(SYNC_GET_TIME, statsToValidate, remoteGetsReadTx != 0 || remoteGetsWriteTx != 0 ? MICROSECONDS : 0, 0, txExecutor);
      assertAttributeValue(NUM_NODES_GET, statsToValidate, remoteGetsReadTx != 0 || remoteGetsWriteTx != 0 ? 1 : 0, 0, txExecutor);
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

   private void assertAttributeValue(ExtendedStatistic attr, EnumSet<ExtendedStatistic> statsToValidate,
                                     double txExecutorValue, double nonTxExecutorValue, int txExecutorIndex) {
      assertTrue(statsToValidate.contains(attr), "Attribute " + attr + " already validated");
      for (int i = 0; i < NUM_NODES; ++i) {
         assertEquals(extendedStatisticInterceptors[i].getAttribute(attr), i == txExecutorIndex ? txExecutorValue : nonTxExecutorValue,
                      "Attribute " + attr + " has wrong value for cache " + i + ".");
      }
      statsToValidate.remove(attr);
   }

   private EnumSet<ExtendedStatistic> getStatsToValidate() {
      EnumSet<ExtendedStatistic> statsToValidate = EnumSet.allOf(ExtendedStatistic.class);
      //this stats are not validated.
      statsToValidate.removeAll(EnumSet.of(PREPARE_COMMAND_SIZE, COMMIT_COMMAND_SIZE, CLUSTERED_GET_COMMAND_SIZE));
      return statsToValidate;
   }

   private void assertAllStatsValidated(EnumSet<ExtendedStatistic> statsToValidate) {
      assertTrue(statsToValidate.isEmpty(), "Stats not validated: " + statsToValidate + ".");
   }

   private enum WriteOperation {
      PUT,
      PUT_IF,
      REPLACE,
      REPLACE_IF,
      REMOVE,
      REMOVE_IF
   }
}
