package org.infinispan.stats;

import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.container.StatisticsSnapshot;
import org.infinispan.stats.logging.Log;
import org.infinispan.stats.percentiles.PercentileStatistic;
import org.infinispan.stats.percentiles.ReservoirSampler;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.EnumMap;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;
import static org.infinispan.stats.percentiles.PercentileStatistic.*;


/**
 * Collects and maintains all the statistics for a cache.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class CacheStatisticCollector {
   private final static Log log = LogFactory.getLog(CacheStatisticCollector.class, Log.class);
   private final TimeService timeService;
   private final ConcurrentGlobalContainer globalContainer;
   private volatile EnumMap<PercentileStatistic, ReservoirSampler> percentiles;

   public CacheStatisticCollector(TimeService timeService) {
      this.timeService = timeService;
      this.globalContainer = new ConcurrentGlobalContainer(timeService);
      reset();
   }

   /**
    * reset all the statistics collected so far.
    */
   public final void reset() {
      if (log.isTraceEnabled()) {
         log.tracef("Resetting Node Scope Statistics");
      }
      globalContainer.reset();
      percentiles = new EnumMap<PercentileStatistic, ReservoirSampler>(PercentileStatistic.class);
      for (PercentileStatistic percentileStatistic : PercentileStatistic.values()) {
         percentiles.put(percentileStatistic, new ReservoirSampler());
      }
   }

   /**
    * Merges a transaction statistics in this cache statistics.
    */
   public final void merge(TransactionStatistics transactionStatistics) {
      if (log.isTraceEnabled()) {
         log.tracef("Merge transaction statistics %s to the node statistics", transactionStatistics);
      }
      ReservoirSampler reservoirSampler;
      ExtendedStatistic percentileSample;
      if (transactionStatistics.isLocalTransaction()) {
         if (transactionStatistics.isReadOnly()) {
            reservoirSampler = percentiles.get(RO_LOCAL_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? RO_TX_SUCCESSFUL_EXECUTION_TIME :
                  RO_TX_ABORTED_EXECUTION_TIME;
         } else {
            reservoirSampler = percentiles.get(WR_LOCAL_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? WR_TX_SUCCESSFUL_EXECUTION_TIME :
                  WR_TX_ABORTED_EXECUTION_TIME;
         }
      } else {
         if (transactionStatistics.isReadOnly()) {
            reservoirSampler = percentiles.get(RO_REMOTE_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? RO_TX_SUCCESSFUL_EXECUTION_TIME :
                  RO_TX_ABORTED_EXECUTION_TIME;
         } else {
            reservoirSampler = percentiles.get(WR_REMOTE_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? WR_TX_SUCCESSFUL_EXECUTION_TIME :
                  WR_TX_ABORTED_EXECUTION_TIME;
         }
      }
      doMerge(transactionStatistics, reservoirSampler, percentileSample);
   }

   /**
    * Adds a value to a local statistic. This value is not associated with any transaction.
    */
   public final void addLocalValue(ExtendedStatistic stat, double value) {
      globalContainer.add(stat, value, true);
   }

   /**
    * Adds a value to a remote statistic. This value is not associated with any transaction.
    */
   public final void addRemoteValue(ExtendedStatistic stat, double value) {
      globalContainer.add(stat, value, false);
   }

   /**
    * @return the percentile og the statistic.
    * @throws IllegalArgumentException if the percentile request is not in the correct bounds (]0,100[)
    */
   public final double getPercentile(PercentileStatistic stat, int percentile)
         throws IllegalArgumentException {
      if (log.isTraceEnabled()) {
         log.tracef("Get percentile %s from %s", percentile, stat);
      }
      return percentiles.get(stat).getKPercentile(percentile);
   }

   /**
    * @return the current value of the statistic. If the statistic is not exported (via JMX), then the sum of the remote
    *         and local value is returned.
    * @throws ExtendedStatisticNotFoundException
    *          if the statistic is not found.
    */
   public final double getAttribute(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      final StatisticsSnapshot snapshot = globalContainer.getSnapshot();
      double value = 0;
      switch (stat) {
         case LOCAL_EXEC_NO_CONT:
            value = microAverageLocal(snapshot, LOCAL_EXEC_NO_CONT, NUM_COMMITTED_WR_TX);
            break;
         case LOCK_HOLD_TIME:
            value = microAverageLocalAndRemote(snapshot, LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case SYNC_PREPARE_TIME:
            value = microAverageLocal(snapshot, SYNC_PREPARE_TIME, NUM_SYNC_PREPARE);
            break;
         case SYNC_COMMIT_TIME:
            value = microAverageLocal(snapshot, SYNC_COMMIT_TIME, NUM_SYNC_COMMIT);
            break;
         case SYNC_ROLLBACK_TIME:
            value = microAverageLocal(snapshot, SYNC_ROLLBACK_TIME, NUM_SYNC_ROLLBACK);
            break;
         case SYNC_GET_TIME:
            value = microAverageLocal(snapshot, SYNC_GET_TIME, NUM_SYNC_GET);
            break;
         case ASYNC_COMMIT_TIME:
            value = microAverageLocal(snapshot, ASYNC_COMMIT_TIME, NUM_ASYNC_COMMIT);
            break;
         case ASYNC_COMPLETE_NOTIFY_TIME:
            value = microAverageLocal(snapshot, ASYNC_COMPLETE_NOTIFY_TIME, NUM_ASYNC_COMPLETE_NOTIFY);
            break;
         case ASYNC_PREPARE_TIME:
            value = microAverageLocal(snapshot, ASYNC_PREPARE_TIME, NUM_ASYNC_PREPARE);
            break;
         case ASYNC_ROLLBACK_TIME:
            value = microAverageLocal(snapshot, ASYNC_ROLLBACK_TIME, NUM_ASYNC_ROLLBACK);
            break;
         case NUM_NODES_COMMIT:
            value = averageLocal(snapshot, NUM_NODES_COMMIT, NUM_SYNC_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case NUM_NODES_GET:
            value = averageLocal(snapshot, NUM_NODES_GET, NUM_SYNC_GET);
            break;
         case NUM_NODES_PREPARE:
            value = averageLocal(snapshot, NUM_NODES_PREPARE, NUM_SYNC_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case NUM_NODES_ROLLBACK:
            value = averageLocal(snapshot, NUM_NODES_ROLLBACK, NUM_SYNC_ROLLBACK, NUM_ASYNC_ROLLBACK);
            break;
         case NUM_NODES_COMPLETE_NOTIFY:
            value = averageLocal(snapshot, NUM_NODES_COMPLETE_NOTIFY, NUM_ASYNC_COMPLETE_NOTIFY);
            break;
         case COMMIT_EXECUTION_TIME:
            value = convertNanosToMicro(averageLocal(snapshot, COMMIT_EXECUTION_TIME, NUM_COMMITTED_WR_TX,
                                                     NUM_COMMITTED_RO_TX));
            break;
         case ROLLBACK_EXECUTION_TIME:
            value = microAverageLocal(snapshot, ROLLBACK_EXECUTION_TIME, NUM_ROLLBACK_COMMAND);
            break;
         case LOCK_WAITING_TIME:
            value = microAverageLocalAndRemote(snapshot, LOCK_WAITING_TIME, NUM_WAITED_FOR_LOCKS);
            break;
         case WRITE_TX_PERCENTAGE: {     //computed on the locally born txs
            double readTx = snapshot.getLocal(NUM_COMMITTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_RO_TX);
            double writeTx = snapshot.getLocal(NUM_COMMITTED_WR_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            double total = readTx + writeTx;
            if (total != 0) {
               value = writeTx / total;
            }
            break;
         }
         case SUCCESSFUL_WRITE_TX_PERCENTAGE: { //computed on the locally born txs
            double readSuxTx = snapshot.getLocal(NUM_COMMITTED_RO_TX);
            double writeSuxTx = snapshot.getLocal(NUM_COMMITTED_WR_TX);
            double total = readSuxTx + writeSuxTx;
            if (total != 0) {
               value = writeSuxTx / total;
            }
            break;
         }
         case NUM_GETS_RO_TX:
            value = averageLocal(snapshot, NUM_GETS_RO_TX, NUM_COMMITTED_RO_TX);
            break;
         case NUM_GETS_WR_TX:
            value = averageLocal(snapshot, NUM_GETS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case NUM_REMOTE_GETS_RO_TX:
            value = averageLocal(snapshot, NUM_REMOTE_GETS_RO_TX, NUM_COMMITTED_RO_TX);
            break;
         case NUM_REMOTE_GETS_WR_TX:
            value = averageLocal(snapshot, NUM_REMOTE_GETS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case REMOTE_GET_EXECUTION:
            value = microAverageLocal(snapshot, NUM_REMOTE_GET, REMOTE_GET_EXECUTION);
            break;
         case NUM_PUTS_WR_TX:
            value = averageLocal(snapshot, NUM_PUTS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case NUM_REMOTE_PUTS_WR_TX:
            value = averageLocal(snapshot, NUM_REMOTE_PUTS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case REMOTE_PUT_EXECUTION:
            value = microAverageLocal(snapshot, NUM_REMOTE_PUT, REMOTE_PUT_EXECUTION);
            break;
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
            value = snapshot.getLocal(stat);
            break;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            value = microAverageLocal(snapshot, WR_TX_SUCCESSFUL_EXECUTION_TIME, NUM_COMMITTED_WR_TX);
            break;
         case WR_TX_ABORTED_EXECUTION_TIME:
            value = microAverageLocal(snapshot, WR_TX_ABORTED_EXECUTION_TIME, NUM_ABORTED_WR_TX);
            break;
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            value = microAverageLocal(snapshot, RO_TX_SUCCESSFUL_EXECUTION_TIME, NUM_COMMITTED_RO_TX);
            break;
         case PREPARE_COMMAND_SIZE:
            value = averageLocal(snapshot, PREPARE_COMMAND_SIZE, NUM_SYNC_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case COMMIT_COMMAND_SIZE:
            value = averageLocal(snapshot, COMMIT_COMMAND_SIZE, NUM_SYNC_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case CLUSTERED_GET_COMMAND_SIZE:
            value = averageLocal(snapshot, NUM_SYNC_GET, CLUSTERED_GET_COMMAND_SIZE);
            break;
         case NUM_LOCK_PER_LOCAL_TX:
            value = averageLocal(snapshot, NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
            break;
         case NUM_LOCK_PER_REMOTE_TX:
            value = averageRemote(snapshot, NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
            break;
         case NUM_HELD_LOCKS_SUCCESS_LOCAL_TX:
            value = averageLocal(snapshot, NUM_HELD_LOCKS_SUCCESS_LOCAL_TX, NUM_COMMITTED_WR_TX);
            break;
         case LOCAL_ROLLBACK_EXECUTION_TIME:
            value = microAverageLocal(snapshot, ROLLBACK_EXECUTION_TIME, NUM_ROLLBACK_COMMAND);
            break;
         case REMOTE_ROLLBACK_EXECUTION_TIME:
            value = microAverageRemote(snapshot, ROLLBACK_EXECUTION_TIME, NUM_ROLLBACK_COMMAND);
            break;
         case LOCAL_COMMIT_EXECUTION_TIME:
            value = microAverageLocal(snapshot, COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
            break;
         case REMOTE_COMMIT_EXECUTION_TIME:
            value = microAverageRemote(snapshot, COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
            break;
         case LOCAL_PREPARE_EXECUTION_TIME:
            value = microAverageLocal(snapshot, PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case REMOTE_PREPARE_EXECUTION_TIME:
            value = microAverageRemote(snapshot, PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case ABORT_RATE:
            double totalAbort = snapshot.getLocal(NUM_ABORTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            double totalCommitAndAbort = snapshot.getLocal(NUM_COMMITTED_RO_TX) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX) + totalAbort;
            if (totalCommitAndAbort != 0) {
               value = totalAbort / totalCommitAndAbort;
            }
            break;
         case ARRIVAL_RATE:
            double localCommittedTx = snapshot.getLocal(NUM_COMMITTED_RO_TX) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX);
            double localAbortedTx = snapshot.getLocal(NUM_ABORTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            double remoteCommittedTx = snapshot.getRemote(NUM_COMMITTED_RO_TX) +
                  snapshot.getRemote(NUM_COMMITTED_WR_TX);
            double remoteAbortedTx = snapshot.getRemote(NUM_ABORTED_RO_TX) +
                  snapshot.getRemote(NUM_ABORTED_WR_TX);
            double totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            value = totalBornTx / convertNanosToSeconds(timeService.timeDuration(snapshot.getLastResetTime(), NANOSECONDS));
            break;
         case THROUGHPUT:
            double totalLocalBornTx = snapshot.getLocal(NUM_COMMITTED_RO_TX) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX);
            value = totalLocalBornTx / convertNanosToSeconds(timeService.timeDuration(snapshot.getLastResetTime(), NANOSECONDS));
            break;
         case LOCK_HOLD_TIME_LOCAL:
            value = microAverageLocal(snapshot, LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case LOCK_HOLD_TIME_REMOTE:
            value = microAverageRemote(snapshot, LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case NUM_COMMITTED_TX:
            value = snapshot.getLocal(NUM_COMMITTED_RO_TX) + snapshot.getLocal(NUM_COMMITTED_WR_TX) +
                  snapshot.getRemote(NUM_COMMITTED_RO_TX) + snapshot.getRemote(NUM_COMMITTED_WR_TX);
            break;
         case NUM_LOCAL_COMMITTED_TX:
            value = snapshot.getLocal(NUM_COMMITTED_RO_TX) + snapshot.getLocal(NUM_COMMITTED_WR_TX);
            break;
         case WRITE_SKEW_PROBABILITY:
            double totalTxs = snapshot.getLocal(NUM_COMMITTED_RO_TX) +
                  snapshot.getLocal(NUM_COMMITTED_WR_TX) +
                  snapshot.getLocal(NUM_ABORTED_RO_TX) +
                  snapshot.getLocal(NUM_ABORTED_WR_TX);
            if (totalTxs != 0) {
               double writeSkew = snapshot.getLocal(NUM_WRITE_SKEW);
               value = writeSkew / totalTxs;
            }
            break;
         case NUM_GET:
            value = snapshot.getLocal(NUM_GETS_WR_TX) +
                  snapshot.getLocal(NUM_GETS_RO_TX);
            break;
         case NUM_REMOTE_GET:
            value = snapshot.getLocal(NUM_REMOTE_GETS_WR_TX) +
                  snapshot.getLocal(NUM_REMOTE_GETS_RO_TX);
            break;
         case NUM_PUT:
            value = snapshot.getLocal(NUM_PUTS_WR_TX);
            break;
         case NUM_REMOTE_PUT:
            value = snapshot.getLocal(NUM_REMOTE_PUTS_WR_TX);
            break;
         case LOCAL_GET_EXECUTION:
            double num = snapshot.getLocal(NUM_GET);
            if (num != 0) {
               double local_get_time = snapshot.getLocal(ALL_GET_EXECUTION) -
                     snapshot.getLocal(SYNC_GET_TIME);
               value = convertNanosToMicro(local_get_time) / num;
            }
            break;
         case RESPONSE_TIME:
            double succWrTot = convertNanosToMicro(snapshot.getLocal(WR_TX_SUCCESSFUL_EXECUTION_TIME));
            double abortWrTot = convertNanosToMicro(snapshot.getLocal(WR_TX_ABORTED_EXECUTION_TIME));
            double succRdTot = convertNanosToMicro(snapshot.getLocal(RO_TX_SUCCESSFUL_EXECUTION_TIME));

            double numWr = snapshot.getLocal(NUM_COMMITTED_WR_TX);
            double numRd = snapshot.getLocal(NUM_COMMITTED_RO_TX);

            if ((numWr + numRd) > 0) {
               value = (succRdTot + succWrTot + abortWrTot) / (numWr + numRd);
            }
            break;
         default:
            if (log.isTraceEnabled()) {
               log.tracef("Attribute %s is not exposed via JMX. Calculating raw value", stat);
            }
            if (stat.isLocal()) {
               value += snapshot.getLocal(stat);
            }
            if (stat.isRemote()) {
               value += snapshot.getRemote(stat);
            }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Get attribute %s = %s", stat, value);
      }
      return value;
   }

   /**
    * Dumps all the cache statistic values to a {@link StringBuilder}
    */
   public final void dumpTo(StringBuilder builder) {
      StringWriter stringWriter = new StringWriter();
      globalContainer.dumpTo(new PrintWriter(stringWriter));
      builder.append(stringWriter.getBuffer());
   }

   /**
    * Prints the cache statistics values to a {@link PrintStream}.
    */
   public final void dumpTo(PrintWriter writer) {
      writer.println("Global Statistics:");
      globalContainer.dumpTo(writer);
   }

   /**
    * @return the conversion of nanoseconds to microseconds without losing precision.
    */
   public static double convertNanosToMicro(double nanos) {
      return nanos / 1E3;
   }

   /**
    * @return the conversion of nanoseconds to seconds without losing precision.
    */
   public static double convertNanosToSeconds(double nanos) {
      return nanos / 1E9;
   }

   private void doMerge(TransactionStatistics transactionStatistics,
                        ReservoirSampler reservoirSampler, ExtendedStatistic percentileSample) {
      transactionStatistics.flushTo(globalContainer);
      try {
         reservoirSampler.insertSample(transactionStatistics.getValue(percentileSample));
      } catch (ExtendedStatisticNotFoundException e) {
         log.extendedStatisticNotFoundForPercentile(percentileSample, e);
      }
   }

   private double averageLocal(StatisticsSnapshot snapshot, ExtendedStatistic numeratorStat,
                               ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += snapshot.getLocal(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = snapshot.getLocal(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double averageRemote(StatisticsSnapshot snapshot, ExtendedStatistic numeratorStat,
                                ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += snapshot.getRemote(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = snapshot.getRemote(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double averageLocalAndRemote(StatisticsSnapshot snapshot, ExtendedStatistic numeratorStat,
                                        ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += snapshot.getRemote(denominatorStat);
         denominator += snapshot.getLocal(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = snapshot.getRemote(numeratorStat);
         numerator += snapshot.getLocal(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double microAverageLocal(StatisticsSnapshot snapshot, ExtendedStatistic numerator,
                                    ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageLocal(snapshot, numerator, denominator));
   }

   private double microAverageRemote(StatisticsSnapshot snapshot, ExtendedStatistic numerator,
                                     ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageRemote(snapshot, numerator, denominator));
   }

   private double microAverageLocalAndRemote(StatisticsSnapshot snapshot, ExtendedStatistic numerator,
                                             ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageLocalAndRemote(snapshot, numerator, denominator));
   }

}
