package org.infinispan.stats.logic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.stats.CacheStatisticCollector;
import org.infinispan.stats.CacheStatisticManager;
import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.wrappers.ExtendedStatisticInterceptor;
import org.infinispan.stats.wrappers.ExtendedStatisticLockManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.DefaultTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.IsolationLevel;
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
@Test(groups = "functional", testName = "stats.logic.LocalTxClusterExtendedStatisticLogicTest")
public class LocalTxClusterExtendedStatisticLogicTest extends SingleCacheManagerTest {

   private static final int SLEEP_TIME = 500;
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
   //private final TransactionInterceptor[] transactionInterceptors = new TransactionInterceptor[NUM_NODES];
   private final List<Object> keys = new ArrayList<Object>(128);
   private ExtendedStatisticInterceptor extendedStatisticInterceptor;

   public final void testPutTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.PUT, 2, 7, 3, 4, 5, false);
   }

   public final void testPutTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.PUT, 3, 6, 2, 5, 4, true);
   }

   public final void testConditionalPutTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.PUT_IF, 4, 5, 4, 6, 3, false);
   }

   public final void testConditionalPutTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.PUT_IF, 5, 4, 5, 7, 2, true);
   }

   public final void testReplaceTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REPLACE, 2, 7, 3, 4, 5, false);
   }

   public final void testReplaceTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REPLACE, 3, 6, 2, 5, 4, true);
   }

   public final void testConditionalReplaceTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REPLACE_IF, 4, 5, 4, 6, 3, false);
   }

   public final void testConditionalReplaceTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REPLACE_IF, 5, 4, 5, 7, 2, true);
   }

   public final void testRemoveTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REMOVE, 2, 7, 3, 4, 5, false);
   }

   public final void testRemoveTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REMOVE, 3, 6, 2, 5, 4, true);
   }

   public final void testConditionalRemoveTxAndReadOnlyTx() throws Exception {
      testStats(WriteOperation.REMOVE_IF, 4, 5, 4, 6, 3, false);
   }

   public final void testConditionalRemoveTxAndReadOnlyTxRollback() throws Exception {
      testStats(WriteOperation.REMOVE_IF, 5, 4, 5, 7, 2, true);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      CacheStatisticManager manager = (CacheStatisticManager) extractField(extendedStatisticInterceptor, "cacheStatisticManager");
      CacheStatisticCollector collector = (CacheStatisticCollector) extractField(manager, "cacheStatisticCollector");
      ConcurrentGlobalContainer globalContainer = (ConcurrentGlobalContainer) extractField(collector, "globalContainer");
      replaceField(TEST_TIME_SERVICE, "timeService", manager, CacheStatisticManager.class);
      replaceField(TEST_TIME_SERVICE, "timeService", collector, CacheStatisticCollector.class);
      replaceField(TEST_TIME_SERVICE, "timeService", globalContainer, ConcurrentGlobalContainer.class);
      replaceField(TEST_TIME_SERVICE, "timeService", extendedStatisticInterceptor, ExtendedStatisticInterceptor.class);
      replaceField(TEST_TIME_SERVICE, "timeService", extractLockManager(cache()), ExtendedStatisticLockManager.class);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.LOCAL, true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false)
            .lockAcquisitionTimeout(0);
      builder.clustering().hash().numOwners(1);
      builder.transaction().recovery().disable();
      extendedStatisticInterceptor = new ExtendedStatisticInterceptor();
      builder.customInterceptors().addInterceptor().interceptor(extendedStatisticInterceptor)
            .after(TxInterceptor.class);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   private void testStats(WriteOperation operation, int numOfWriteTx, int numOfWrites, int numOfReadsPerWriteTx,
                          int numOfReadOnlyTx, int numOfReadPerReadTx, boolean abort)
         throws Exception {
      for (int i = 1; i <= (numOfReadsPerWriteTx + numOfWrites) * numOfWriteTx + numOfReadPerReadTx * numOfReadOnlyTx; ++i) {
         cache().put(getKey(i), getInitValue(i));
      }
      sleepThread(SLEEP_TIME);
      resetStats();
      int localGetsReadTx = 0;
      int localGetsWriteTx = 0;
      int localPuts = 0;
      int localLocks = 0;
      int numOfLocalWriteTx = 0;

      int keyIndex = 0;
      //write tx
      for (int tx = 1; tx <= numOfWriteTx; ++tx) {
         tm().begin();
         for (int i = 1; i <= numOfReadsPerWriteTx; ++i) {
            keyIndex++;
            Object key = getKey(keyIndex);
            localGetsWriteTx++;
            assertEquals(cache().get(key), getInitValue(keyIndex));
         }
         for (int i = 1; i <= numOfWrites; ++i) {
            keyIndex++;
            Object key = operation == WriteOperation.PUT_IF ? getKey(-keyIndex) : getKey(keyIndex);
            switch (operation) {
               case PUT:
                  cache().put(key, getValue(keyIndex));
                  break;
               case PUT_IF:
                  cache().putIfAbsent(key, getValue(keyIndex));
                  break;
               case REPLACE:
                  cache().replace(key, getValue(keyIndex));
                  break;
               case REPLACE_IF:
                  cache().replace(key, getInitValue(keyIndex), getValue(keyIndex));
                  break;
               case REMOVE:
                  cache().remove(key);
                  break;
               case REMOVE_IF:
                  cache().remove(key, getInitValue(keyIndex));
                  break;
               default:
                  //nothing
            }
            localPuts++;
            if (!abort) {
               localLocks++;
            }
         }
         numOfLocalWriteTx++;
         if (abort) {
            tm().rollback();
         } else {
            tm().commit();

         }
      }
      sleepThread(SLEEP_TIME);

      //read tx
      for (int tx = 1; tx <= numOfReadOnlyTx; ++tx) {
         tm().begin();
         for (int i = 1; i <= numOfReadPerReadTx; ++i) {
            keyIndex++;
            Object key = getKey(keyIndex);
            localGetsReadTx++;
            assertEquals(cache().get(key), getInitValue(keyIndex));
         }
         if (abort) {
            tm().rollback();
         } else {
            tm().commit();

         }
      }
      sleepThread(SLEEP_TIME);

      EnumSet<ExtendedStatistic> statsToValidate = getStatsToValidate();

      assertTxValues(statsToValidate, numOfLocalWriteTx, numOfReadOnlyTx, abort);
      assertLockingValues(statsToValidate, localLocks, numOfLocalWriteTx, abort);
      assertAccessesValues(statsToValidate, localGetsReadTx, localGetsWriteTx, localPuts, numOfWriteTx, numOfReadOnlyTx, abort);

      assertAttributeValue(NUM_WRITE_SKEW, statsToValidate, 0);
      assertAttributeValue(WRITE_SKEW_PROBABILITY, statsToValidate, 0);

      assertAllStatsValidated(statsToValidate);
      resetStats();
   }

   private Object getKey(int i) {
      if (i < 0) {
         return "KEY_" + i;
      }
      for (int j = keys.size(); j <= i; ++j) {
         keys.add("KEY_" + (j + 1));
      }
      return keys.get(i - 1);
   }

   private Object getInitValue(int i) {
      return "INIT_" + i;
   }

   private Object getValue(int i) {
      return "VALUE_" + i;
   }

   private void assertTxValues(EnumSet<ExtendedStatistic> statsToValidate, int numOfWriteTx,
                               int numOfReadTx, boolean abort) {
      log.infof("Check Tx value: writeTx=%s, readTx=%s, abort?=%s", numOfWriteTx, numOfReadTx, abort);
      if (abort) {
         assertAttributeValue(NUM_COMMITTED_RO_TX, statsToValidate, 0); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_WR_TX, statsToValidate, 0); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_WR_TX, statsToValidate, numOfWriteTx); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_RO_TX, statsToValidate, numOfReadTx); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_TX, statsToValidate, 0);
         assertAttributeValue(NUM_LOCAL_COMMITTED_TX, statsToValidate, 0);
         assertAttributeValue(LOCAL_EXEC_NO_CONT, statsToValidate, 0);
         assertAttributeValue(WRITE_TX_PERCENTAGE, statsToValidate, numOfWriteTx * 1.0 / (numOfWriteTx + numOfReadTx));
         assertAttributeValue(SUCCESSFUL_WRITE_TX_PERCENTAGE, statsToValidate, 0);
         assertAttributeValue(WR_TX_ABORTED_EXECUTION_TIME, statsToValidate, numOfWriteTx != 0 ? MICROSECONDS : 0);
         assertAttributeValue(RO_TX_ABORTED_EXECUTION_TIME, statsToValidate, numOfReadTx); //not exposed via JMX
         assertAttributeValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(ABORT_RATE, statsToValidate, 1);
         assertAttributeValue(ARRIVAL_RATE, statsToValidate, (numOfWriteTx + numOfReadTx) / SECONDS);
         assertAttributeValue(THROUGHPUT, statsToValidate, 0);
         assertAttributeValue(ROLLBACK_EXECUTION_TIME, statsToValidate, numOfReadTx != 0 || numOfWriteTx != 0 ? MICROSECONDS : 0);
         assertAttributeValue(NUM_ROLLBACK_COMMAND, statsToValidate, numOfReadTx + numOfWriteTx);
         assertAttributeValue(LOCAL_ROLLBACK_EXECUTION_TIME, statsToValidate, numOfReadTx != 0 || numOfWriteTx != 0 ? MICROSECONDS : 0);
         assertAttributeValue(REMOTE_ROLLBACK_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(COMMIT_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_COMMIT_COMMAND, statsToValidate, 0);
         assertAttributeValue(LOCAL_COMMIT_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(REMOTE_COMMIT_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(PREPARE_EXECUTION_TIME, statsToValidate, 0); // //not exposed via JMX
         assertAttributeValue(NUM_PREPARE_COMMAND, statsToValidate, 0);
         assertAttributeValue(LOCAL_PREPARE_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(REMOTE_PREPARE_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_SYNC_PREPARE, statsToValidate, 0);
         assertAttributeValue(SYNC_PREPARE_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_SYNC_COMMIT, statsToValidate, 0);
         assertAttributeValue(SYNC_COMMIT_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_SYNC_ROLLBACK, statsToValidate, 0);
         assertAttributeValue(SYNC_ROLLBACK_TIME, statsToValidate, 0);
         assertAttributeValue(ASYNC_PREPARE_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_PREPARE, statsToValidate, 0);
         assertAttributeValue(ASYNC_COMMIT_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_COMMIT, statsToValidate, 0);
         assertAttributeValue(ASYNC_ROLLBACK_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_ROLLBACK, statsToValidate, 0);
         assertAttributeValue(ASYNC_COMPLETE_NOTIFY_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_COMPLETE_NOTIFY, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_PREPARE, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_COMMIT, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_ROLLBACK, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_COMPLETE_NOTIFY, statsToValidate, 0);
         assertAttributeValue(RESPONSE_TIME, statsToValidate, 0);
      } else {
         assertAttributeValue(NUM_COMMITTED_RO_TX, statsToValidate, numOfReadTx); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_WR_TX, statsToValidate, numOfWriteTx); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_WR_TX, statsToValidate, 0); //not exposed via JMX
         assertAttributeValue(NUM_ABORTED_RO_TX, statsToValidate, 0); //not exposed via JMX
         assertAttributeValue(NUM_COMMITTED_TX, statsToValidate, numOfWriteTx + numOfReadTx);
         assertAttributeValue(NUM_LOCAL_COMMITTED_TX, statsToValidate, numOfReadTx + numOfWriteTx);
         assertAttributeValue(LOCAL_EXEC_NO_CONT, statsToValidate, numOfWriteTx != 0 ? MICROSECONDS : 0);
         assertAttributeValue(WRITE_TX_PERCENTAGE, statsToValidate, (numOfWriteTx * 1.0) / (numOfReadTx + numOfWriteTx));
         assertAttributeValue(SUCCESSFUL_WRITE_TX_PERCENTAGE, statsToValidate, (numOfReadTx + numOfWriteTx) > 0 ? (numOfWriteTx * 1.0) / (numOfReadTx + numOfWriteTx) : 0);
         assertAttributeValue(WR_TX_ABORTED_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(RO_TX_ABORTED_EXECUTION_TIME, statsToValidate, 0); //not exposed via JMX
         assertAttributeValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, numOfWriteTx != 0 ? MICROSECONDS : 0);
         assertAttributeValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, statsToValidate, numOfReadTx != 0 ? MICROSECONDS : 0);
         assertAttributeValue(ABORT_RATE, statsToValidate, 0);
         assertAttributeValue(ARRIVAL_RATE, statsToValidate, (numOfWriteTx + numOfReadTx) / SECONDS);
         assertAttributeValue(THROUGHPUT, statsToValidate, (numOfWriteTx + numOfReadTx) / SECONDS);
         assertAttributeValue(ROLLBACK_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ROLLBACK_COMMAND, statsToValidate, 0);
         assertAttributeValue(LOCAL_ROLLBACK_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(REMOTE_ROLLBACK_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(COMMIT_EXECUTION_TIME, statsToValidate, (numOfReadTx != 0 || numOfWriteTx != 0) ? MICROSECONDS : 0);
         assertAttributeValue(NUM_COMMIT_COMMAND, statsToValidate, numOfReadTx + numOfWriteTx);
         assertAttributeValue(LOCAL_COMMIT_EXECUTION_TIME, statsToValidate, (numOfReadTx != 0 || numOfWriteTx != 0) ? MICROSECONDS : 0);
         assertAttributeValue(REMOTE_COMMIT_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(PREPARE_EXECUTION_TIME, statsToValidate, numOfReadTx + numOfWriteTx); // //not exposed via JMX
         assertAttributeValue(NUM_PREPARE_COMMAND, statsToValidate, numOfReadTx + numOfWriteTx);
         assertAttributeValue(LOCAL_PREPARE_EXECUTION_TIME, statsToValidate, (numOfReadTx != 0 || numOfWriteTx != 0) ? MICROSECONDS : 0);
         assertAttributeValue(REMOTE_PREPARE_EXECUTION_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_SYNC_PREPARE, statsToValidate, 0);
         assertAttributeValue(SYNC_PREPARE_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_SYNC_COMMIT, statsToValidate, 0);
         assertAttributeValue(SYNC_COMMIT_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_SYNC_ROLLBACK, statsToValidate, 0);
         assertAttributeValue(SYNC_ROLLBACK_TIME, statsToValidate, 0);
         assertAttributeValue(ASYNC_PREPARE_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_PREPARE, statsToValidate, 0);
         assertAttributeValue(ASYNC_COMMIT_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_COMMIT, statsToValidate, 0);
         assertAttributeValue(ASYNC_ROLLBACK_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_ROLLBACK, statsToValidate, 0);
         assertAttributeValue(ASYNC_COMPLETE_NOTIFY_TIME, statsToValidate, 0);
         assertAttributeValue(NUM_ASYNC_COMPLETE_NOTIFY, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_PREPARE, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_COMMIT, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_ROLLBACK, statsToValidate, 0);
         assertAttributeValue(NUM_NODES_COMPLETE_NOTIFY, statsToValidate, 0);
         assertAttributeValue(RESPONSE_TIME, statsToValidate, numOfReadTx != 0 || numOfWriteTx != 0 ? MICROSECONDS : 0);
      }
   }

   private void assertLockingValues(EnumSet<ExtendedStatistic> statsToValidate, int numOfLocks,
                                    int numOfWriteTx, boolean abort) {
      log.infof("Check Locking value. locks=%s, writeTx=%s, abort?=%s", numOfLocks, numOfWriteTx, abort);
      //remote puts always acquire locks
      assertAttributeValue(LOCK_HOLD_TIME_LOCAL, statsToValidate, numOfLocks != 0 ? MICROSECONDS : 0);
      assertAttributeValue(LOCK_HOLD_TIME_REMOTE, statsToValidate, 0);
      assertAttributeValue(NUM_LOCK_PER_LOCAL_TX, statsToValidate, numOfWriteTx != 0 ? numOfLocks * 1.0 / numOfWriteTx : 0);
      assertAttributeValue(NUM_LOCK_PER_REMOTE_TX, statsToValidate, 0);
      assertAttributeValue(LOCK_HOLD_TIME_SUCCESS_LOCAL_TX, statsToValidate, 0);
      assertAttributeValue(NUM_HELD_LOCKS_SUCCESS_LOCAL_TX, statsToValidate, !abort && numOfWriteTx != 0 ? numOfLocks * 1.0 / numOfWriteTx : 0);
      assertAttributeValue(LOCK_HOLD_TIME, statsToValidate, numOfLocks != 0 ? MICROSECONDS : 0);
      assertAttributeValue(NUM_HELD_LOCKS, statsToValidate, numOfLocks);
      assertAttributeValue(NUM_WAITED_FOR_LOCKS, statsToValidate, 0);
      assertAttributeValue(LOCK_WAITING_TIME, statsToValidate, 0);
      assertAttributeValue(NUM_LOCK_FAILED_TIMEOUT, statsToValidate, 0);
      assertAttributeValue(NUM_LOCK_FAILED_DEADLOCK, statsToValidate, 0);
   }

   private void assertAccessesValues(EnumSet<ExtendedStatistic> statsToValidate, int getsReadTx, int getsWriteTx, int puts,
                                     int numOfWriteTx, int numOfReadTx, boolean abort) {
      log.infof("Check accesses values. getsReadTx=%s, getsWriteTx=%s, puts=%s, writeTx=%s, readTx=%s, abort?=%s",
                getsReadTx, getsWriteTx, puts, numOfWriteTx, numOfReadTx, abort);
      assertAttributeValue(NUM_REMOTE_PUT, statsToValidate, 0);
      assertAttributeValue(REMOTE_PUT_EXECUTION, statsToValidate, 0);
      assertAttributeValue(LOCAL_PUT_EXECUTION, statsToValidate, 0);
      assertAttributeValue(NUM_PUT, statsToValidate, puts);
      assertAttributeValue(NUM_PUTS_WR_TX, statsToValidate, !abort && numOfWriteTx != 0 ? puts * 1.0 / numOfWriteTx : 0);
      assertAttributeValue(NUM_REMOTE_PUTS_WR_TX, statsToValidate, 0);

      assertAttributeValue(NUM_REMOTE_GET, statsToValidate, 0);
      assertAttributeValue(NUM_GET, statsToValidate, getsReadTx + getsWriteTx);
      assertAttributeValue(NUM_GETS_RO_TX, statsToValidate, !abort && numOfReadTx != 0 ? getsReadTx * 1.0 / numOfReadTx : 0);
      assertAttributeValue(NUM_GETS_WR_TX, statsToValidate, !abort && numOfWriteTx != 0 ? getsWriteTx * 1.0 / numOfWriteTx : 0);
      assertAttributeValue(NUM_REMOTE_GETS_WR_TX, statsToValidate, 0);
      assertAttributeValue(NUM_REMOTE_GETS_RO_TX, statsToValidate, 0);
      assertAttributeValue(ALL_GET_EXECUTION, statsToValidate, getsReadTx + getsWriteTx);
      //always zero because the all get execution and the rtt is always 1 (all get execution - rtt == 0)
      assertAttributeValue(LOCAL_GET_EXECUTION, statsToValidate, getsReadTx != 0 || getsWriteTx != 0 ? MICROSECONDS : 0);
      assertAttributeValue(REMOTE_GET_EXECUTION, statsToValidate, 0);
      assertAttributeValue(NUM_SYNC_GET, statsToValidate, 0);
      assertAttributeValue(SYNC_GET_TIME, statsToValidate, 0);
      assertAttributeValue(NUM_NODES_GET, statsToValidate, 0);
   }

   private void resetStats() {
      extendedStatisticInterceptor.resetStatistics();
      for (ExtendedStatistic extendedStatistic : values()) {
         assertEquals(extendedStatisticInterceptor.getAttribute(extendedStatistic), 0.0, "Attribute " + extendedStatistic +
               " is not zero after reset");
      }

   }

   private void assertAttributeValue(ExtendedStatistic attr, EnumSet<ExtendedStatistic> statsToValidate,
                                     double txExecutorValue) {
      assertTrue(statsToValidate.contains(attr), "Attribute " + attr + " already validated");
      assertEquals(extendedStatisticInterceptor.getAttribute(attr), txExecutorValue, "Attribute " + attr +
            " has wrong value for cache.");
      statsToValidate.remove(attr);
   }

   private EnumSet<ExtendedStatistic> getStatsToValidate() {
      EnumSet<ExtendedStatistic> statsToValidate = EnumSet.allOf(ExtendedStatistic.class);
      //TODO fix this
      statsToValidate.removeAll(EnumSet.of(PREPARE_COMMAND_SIZE, COMMIT_COMMAND_SIZE,
                                           CLUSTERED_GET_COMMAND_SIZE));
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
