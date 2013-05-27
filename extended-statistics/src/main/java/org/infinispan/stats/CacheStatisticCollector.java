package org.infinispan.stats;

import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.container.ExtendedStatisticsContainer;
import org.infinispan.stats.container.LocalExtendedStatisticsContainer;
import org.infinispan.stats.container.RemoteExtendedStatisticsContainer;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;
import org.infinispan.stats.exception.PercentileOutOfBounds;
import org.infinispan.stats.percentiles.PercentileStatistic;
import org.infinispan.stats.percentiles.ReservoirSampler;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.PrintStream;
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
 * @since 5.3
 */
public class CacheStatisticCollector {
   private final static Log log = LogFactory.getLog(CacheStatisticCollector.class);
   private final TimeService timeService;
   private LocalExtendedStatisticsContainer localContainer;
   private RemoteExtendedStatisticsContainer remoteContainer;
   private EnumMap<PercentileStatistic, ReservoirSampler> percentiles;
   private long lastResetTime;

   public CacheStatisticCollector(TimeService timeService) {
      this.timeService = timeService;
      reset();
   }

   /**
    * reset all the statistics collected so far.
    */
   public final synchronized void reset() {
      if (log.isTraceEnabled()) {
         log.tracef("Resetting Node Scope Statistics");
      }
      this.localContainer = new LocalExtendedStatisticsContainer();
      this.remoteContainer = new RemoteExtendedStatisticsContainer();
      percentiles = new EnumMap<PercentileStatistic, ReservoirSampler>(PercentileStatistic.class);
      for (PercentileStatistic percentileStatistic : PercentileStatistic.values()) {
         percentiles.put(percentileStatistic, new ReservoirSampler());
      }
      this.lastResetTime = timeService.time();
   }

   /**
    * Merges a transaction statistics in this cache statistics.
    *
    * @param transactionStatistics
    */
   public final synchronized void merge(TransactionStatistics transactionStatistics) {
      if (log.isTraceEnabled()) {
         log.tracef("Merge transaction statistics %s to the node statistics", transactionStatistics);
      }
      ExtendedStatisticsContainer container;
      ReservoirSampler reservoirSampler;
      ExtendedStatistic percentileSample;
      if (transactionStatistics.isLocalTransaction()) {
         container = localContainer;
         if (transactionStatistics.isReadOnly()) {
            reservoirSampler = percentiles.get(RO_LOCAL_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? RO_TX_SUCCESSFUL_EXECUTION_TIME : RO_TX_ABORTED_EXECUTION_TIME;
         } else {
            reservoirSampler = percentiles.get(WR_LOCAL_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? WR_TX_SUCCESSFUL_EXECUTION_TIME : WR_TX_ABORTED_EXECUTION_TIME;
         }
      } else {
         container = remoteContainer;
         if (transactionStatistics.isReadOnly()) {
            reservoirSampler = percentiles.get(RO_REMOTE_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? RO_TX_SUCCESSFUL_EXECUTION_TIME : RO_TX_ABORTED_EXECUTION_TIME;
         } else {
            reservoirSampler = percentiles.get(WR_REMOTE_EXECUTION);
            percentileSample = transactionStatistics.isCommitted() ? WR_TX_SUCCESSFUL_EXECUTION_TIME : WR_TX_ABORTED_EXECUTION_TIME;
         }
      }
      doMerge(transactionStatistics, container, reservoirSampler, percentileSample);
   }

   /**
    * Adds a value to a local statistic. This value is not associated with any transaction.
    *
    * @param stat
    * @param value
    */
   public final synchronized void addLocalValue(ExtendedStatistic stat, double value) {
      localContainer.addValue(stat, value);
   }

   /**
    * Adds a value to a remote statistic. This value is not associated with any transaction.
    *
    * @param stat
    * @param value
    */
   public final synchronized void addRemoteValue(ExtendedStatistic stat, double value) {
      remoteContainer.addValue(stat, value);
   }

   /**
    * @param stat
    * @param percentile
    * @return the percentile og the statistic.
    * @throws PercentileOutOfBounds if the percentile request is not in the correct bounds (]0,100[)
    */
   public final synchronized double getPercentile(PercentileStatistic stat, int percentile) throws PercentileOutOfBounds {
      if (log.isTraceEnabled()) {
         log.tracef("Get percentile %s from %s", percentile, stat);
      }
      return percentiles.get(stat).getKPercentile(percentile);
   }

   /**
    * @param stat
    * @return the current value of the statistic. If the statistic is not exported (via JMX), then the sum of the remote
    *         and local value is returned.
    * @throws ExtendedStatisticNotFoundException
    *          if the statistic is not found.
    */
   public final synchronized double getAttribute(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      double value = 0;
      switch (stat) {
         case LOCAL_EXEC_NO_CONT:
            value = microAverageLocal(LOCAL_EXEC_NO_CONT, NUM_COMMITTED_WR_TX);
            break;
         case LOCK_HOLD_TIME:
            value = microAverageLocalAndRemote(LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case RTT_PREPARE:
            value = microAverageLocal(RTT_PREPARE, NUM_RTTS_PREPARE);
            break;
         case RTT_COMMIT:
            value = microAverageLocal(RTT_COMMIT, NUM_RTTS_COMMIT);
            break;
         case RTT_ROLLBACK:
            value = microAverageLocal(RTT_ROLLBACK, NUM_RTTS_ROLLBACK);
            break;
         case RTT_GET:
            value = microAverageLocal(RTT_GET, NUM_RTTS_GET);
            break;
         case ASYNC_COMMIT:
            value = microAverageLocal(ASYNC_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case ASYNC_COMPLETE_NOTIFY:
            value = microAverageLocal(ASYNC_COMPLETE_NOTIFY, NUM_ASYNC_COMPLETE_NOTIFY);
            break;
         case ASYNC_PREPARE:
            value = microAverageLocal(ASYNC_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case ASYNC_ROLLBACK:
            value = microAverageLocal(ASYNC_ROLLBACK, NUM_ASYNC_ROLLBACK);
            break;
         case NUM_NODES_COMMIT:
            value = averageLocal(NUM_NODES_COMMIT, NUM_RTTS_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case NUM_NODES_GET:
            value = averageLocal(NUM_NODES_GET, NUM_RTTS_GET);
            break;
         case NUM_NODES_PREPARE:
            value = averageLocal(NUM_NODES_PREPARE, NUM_RTTS_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case NUM_NODES_ROLLBACK:
            value = averageLocal(NUM_NODES_ROLLBACK, NUM_RTTS_ROLLBACK, NUM_ASYNC_ROLLBACK);
            break;
         case NUM_NODES_COMPLETE_NOTIFY:
            value = averageLocal(NUM_NODES_COMPLETE_NOTIFY, NUM_ASYNC_COMPLETE_NOTIFY);
            break;
         case PUTS_PER_LOCAL_TX:
            value = averageLocal(NUM_SUCCESSFUL_PUTS, NUM_COMMITTED_WR_TX);
            break;
         case COMMIT_EXECUTION_TIME:
            value = convertNanosToMicro(averageLocal(COMMIT_EXECUTION_TIME, NUM_COMMITTED_WR_TX, NUM_COMMITTED_RO_TX));
            break;
         case ROLLBACK_EXECUTION_TIME:
            value = microAverageLocal(ROLLBACK_EXECUTION_TIME, NUM_ROLLBACKS);
            break;
         case LOCK_WAITING_TIME:
            value = microAverageLocalAndRemote(LOCK_WAITING_TIME, NUM_WAITED_FOR_LOCKS);
            break;
         case TX_WRITE_PERCENTAGE: {     //computed on the locally born txs
            double readTx = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_RO_TX);
            double writeTx = localContainer.getValue(NUM_COMMITTED_WR_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            double total = readTx + writeTx;
            if (total != 0) {
               value = writeTx / total;
            }
            break;
         }
         case SUCCESSFUL_WRITE_PERCENTAGE: { //computed on the locally born txs
            double readSuxTx = localContainer.getValue(NUM_COMMITTED_RO_TX);
            double writeSuxTx = localContainer.getValue(NUM_COMMITTED_WR_TX);
            double total = readSuxTx + writeSuxTx;
            if (total != 0) {
               value = writeSuxTx / total;
            }
            break;
         }
         case NUM_SUCCESSFUL_GETS_RO_TX:
            value = averageLocal(NUM_SUCCESSFUL_GETS_RO_TX, NUM_COMMITTED_RO_TX);
            break;
         case NUM_SUCCESSFUL_GETS_WR_TX:
            value = averageLocal(NUM_SUCCESSFUL_GETS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case NUM_SUCCESSFUL_REMOTE_GETS_RO_TX:
            value = averageLocal(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX, NUM_COMMITTED_RO_TX);
            break;
         case NUM_SUCCESSFUL_REMOTE_GETS_WR_TX:
            value = averageLocal(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case REMOTE_GET_EXECUTION:
            value = microAverageLocal(NUM_REMOTE_GET, REMOTE_GET_EXECUTION);
            break;
         case NUM_SUCCESSFUL_PUTS_WR_TX:
            value = averageLocal(NUM_SUCCESSFUL_PUTS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX:
            value = averageLocal(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX, NUM_COMMITTED_WR_TX);
            break;
         case REMOTE_PUT_EXECUTION:
            value = microAverageLocal(NUM_REMOTE_PUT, REMOTE_PUT_EXECUTION);
            break;
         case NUM_LOCK_FAILED_DEADLOCK:
         case NUM_LOCK_FAILED_TIMEOUT:
            value = localContainer.getValue(stat);
            break;
         case WR_TX_LOCAL_EXECUTION_TIME:
            value = microAverageLocal(WR_TX_LOCAL_EXECUTION_TIME, NUM_COMMITTED_WR_TX);
            break;
         case WR_TX_SUCCESSFUL_EXECUTION_TIME:
            value = microAverageLocal(WR_TX_SUCCESSFUL_EXECUTION_TIME, NUM_COMMITTED_WR_TX);
            break;
         case WR_TX_ABORTED_EXECUTION_TIME:
            value = microAverageLocal(WR_TX_ABORTED_EXECUTION_TIME, NUM_ABORTED_WR_TX);
            break;
         case RO_TX_SUCCESSFUL_EXECUTION_TIME:
            value = microAverageLocal(RO_TX_SUCCESSFUL_EXECUTION_TIME, NUM_COMMITTED_RO_TX);
            break;
         case PREPARE_COMMAND_SIZE:
            value = averageLocal(PREPARE_COMMAND_SIZE, NUM_RTTS_PREPARE, NUM_ASYNC_PREPARE);
            break;
         case COMMIT_COMMAND_SIZE:
            value = averageLocal(COMMIT_COMMAND_SIZE, NUM_RTTS_COMMIT, NUM_ASYNC_COMMIT);
            break;
         case CLUSTERED_GET_COMMAND_SIZE:
            value = averageLocal(NUM_RTTS_GET, CLUSTERED_GET_COMMAND_SIZE);
            break;
         case NUM_LOCK_PER_LOCAL_TX:
            value = averageLocal(NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
            break;
         case NUM_LOCK_PER_REMOTE_TX:
            value = averageRemote(NUM_HELD_LOCKS, NUM_COMMITTED_WR_TX, NUM_ABORTED_WR_TX);
            break;
         case NUM_LOCK_PER_SUCCESS_LOCAL_TX:
            value = averageLocal(NUM_HELD_LOCKS_SUCCESS_TX, NUM_COMMITTED_WR_TX);
            break;
         case LOCAL_ROLLBACK_EXECUTION_TIME:
            value = microAverageLocal(ROLLBACK_EXECUTION_TIME, NUM_ROLLBACKS);
            break;
         case REMOTE_ROLLBACK_EXECUTION_TIME:
            value = microAverageRemote(ROLLBACK_EXECUTION_TIME, NUM_ROLLBACKS);
            break;
         case LOCAL_COMMIT_EXECUTION_TIME:
            value = microAverageLocal(COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
            break;
         case REMOTE_COMMIT_EXECUTION_TIME:
            value = microAverageRemote(COMMIT_EXECUTION_TIME, NUM_COMMIT_COMMAND);
            break;
         case LOCAL_PREPARE_EXECUTION_TIME:
            value = microAverageLocal(PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case REMOTE_PREPARE_EXECUTION_TIME:
            value = microAverageRemote(PREPARE_EXECUTION_TIME, NUM_PREPARE_COMMAND);
            break;
         case ABORT_RATE:
            double totalAbort = localContainer.getValue(NUM_ABORTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            double totalCommitAndAbort = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX) + totalAbort;
            if (totalCommitAndAbort != 0) {
               value = totalAbort / totalCommitAndAbort;
            }
            break;
         case ARRIVAL_RATE:
            double localCommittedTx = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX);
            double localAbortedTx = localContainer.getValue(NUM_ABORTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            double remoteCommittedTx = remoteContainer.getValue(NUM_COMMITTED_RO_TX) +
                  remoteContainer.getValue(NUM_COMMITTED_WR_TX);
            double remoteAbortedTx = remoteContainer.getValue(NUM_ABORTED_RO_TX) +
                  remoteContainer.getValue(NUM_ABORTED_WR_TX);
            double totalBornTx = localAbortedTx + localCommittedTx + remoteAbortedTx + remoteCommittedTx;
            value = totalBornTx / convertNanosToSeconds(timeService.timeDuration(lastResetTime, NANOSECONDS));
            break;
         case THROUGHPUT:
            double totalLocalBornTx = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX);
            value = totalLocalBornTx / convertNanosToSeconds(timeService.timeDuration(lastResetTime, NANOSECONDS));
            break;
         case LOCK_HOLD_TIME_LOCAL:
            value = microAverageLocal(LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case LOCK_HOLD_TIME_REMOTE:
            value = microAverageRemote(LOCK_HOLD_TIME, NUM_HELD_LOCKS);
            break;
         case NUM_COMMITS:
            value = localContainer.getValue(NUM_COMMITTED_RO_TX) + localContainer.getValue(NUM_COMMITTED_WR_TX) +
                  remoteContainer.getValue(NUM_COMMITTED_RO_TX) + remoteContainer.getValue(NUM_COMMITTED_WR_TX);
            break;
         case NUM_LOCAL_COMMITS:
            value = localContainer.getValue(NUM_COMMITTED_RO_TX) + localContainer.getValue(NUM_COMMITTED_WR_TX);
            break;
         case WRITE_SKEW_PROBABILITY:
            double totalTxs = localContainer.getValue(NUM_COMMITTED_RO_TX) +
                  localContainer.getValue(NUM_COMMITTED_WR_TX) +
                  localContainer.getValue(NUM_ABORTED_RO_TX) +
                  localContainer.getValue(NUM_ABORTED_WR_TX);
            if (totalTxs != 0) {
               double writeSkew = localContainer.getValue(NUM_WRITE_SKEW);
               value = writeSkew / totalTxs;
            }
            break;
         case NUM_GET:
            value = localContainer.getValue(NUM_SUCCESSFUL_GETS_WR_TX) +
                  localContainer.getValue(NUM_SUCCESSFUL_GETS_RO_TX);
            break;
         case NUM_REMOTE_GET:
            value = localContainer.getValue(NUM_SUCCESSFUL_REMOTE_GETS_WR_TX) +
                  localContainer.getValue(NUM_SUCCESSFUL_REMOTE_GETS_RO_TX);
            break;
         case NUM_PUT:
            value = localContainer.getValue(NUM_SUCCESSFUL_PUTS_WR_TX);
            break;
         case NUM_REMOTE_PUT:
            value = localContainer.getValue(NUM_SUCCESSFUL_REMOTE_PUTS_WR_TX);
            break;
         case LOCAL_GET_EXECUTION:
            double num = localContainer.getValue(NUM_GET);
            if (num != 0) {
               double local_get_time = localContainer.getValue(ALL_GET_EXECUTION) -
                     localContainer.getValue(RTT_GET);
               value = convertNanosToMicro(local_get_time) / num;
            }
            break;
         case RESPONSE_TIME:
            double succWrTot = convertNanosToMicro(localContainer.getValue(WR_TX_SUCCESSFUL_EXECUTION_TIME));
            double abortWrTot = convertNanosToMicro(localContainer.getValue(WR_TX_ABORTED_EXECUTION_TIME));
            double succRdTot = convertNanosToMicro(localContainer.getValue(RO_TX_SUCCESSFUL_EXECUTION_TIME));

            double numWr = localContainer.getValue(NUM_COMMITTED_WR_TX);
            double numRd = localContainer.getValue(NUM_COMMITTED_RO_TX);

            if ((numWr + numRd) > 0) {
               value = (succRdTot + succWrTot + abortWrTot) / (numWr + numRd);
            }
            break;
         default:
            if (log.isTraceEnabled()) {
               log.tracef("Attribute %s is not exposed via JMX. Calculating raw value", stat);
            }
            if (stat.isLocal()) {
               value += localContainer.getValue(stat);
            }
            if (stat.isRemote()) {
               value += remoteContainer.getValue(stat);
            }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Get attribute %s = %s", stat, value);
      }
      return value;
   }

   /**
    * @return a String with all the cache statistic values.
    */
   public final void dumpTo(StringBuilder builder) {
      builder.append("Local Statistics:")
            .append(System.getProperty("line.separator"));
      localContainer.dumpTo(builder);
      builder.append(System.getProperty("line.separator"))
            .append("Remote Statistics:")
            .append(System.getProperty("line.separator"));
      remoteContainer.dumpTo(builder);
   }

   /**
    * Prints the cache statistics values to a {@link PrintStream}.
    *
    * @param stream
    */
   public final void dumpTo(PrintStream stream) {
      stream.println("Local Statistics:");
      localContainer.dumpTo(stream);
      stream.println();
      stream.println("Remote Statistics:");
      remoteContainer.dumpTo(stream);
   }

   /**
    * @param nanos
    * @return the conversion of nanoseconds to microseconds without losing precision.
    */
   public static double convertNanosToMicro(double nanos) {
      return nanos / 1E3;
   }

   /**
    * @param nanos
    * @return the conversion of nanoseconds to seconds without losing precision.
    */
   public static double convertNanosToSeconds(double nanos) {
      return nanos / 1E9;
   }

   private void doMerge(TransactionStatistics transactionStatistics, ExtendedStatisticsContainer container,
                        ReservoirSampler reservoirSampler, ExtendedStatistic percentileSample) {
      transactionStatistics.flushTo(container);
      try {
         reservoirSampler.insertSample(transactionStatistics.getValue(percentileSample));
      } catch (ExtendedStatisticNotFoundException e) {
         log.warnf("Extended Statistic not found while tried to add a percentile sample. %s", e.getLocalizedMessage());
      }
   }

   private double averageLocal(ExtendedStatistic numeratorStat, ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += localContainer.getValue(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = localContainer.getValue(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double averageRemote(ExtendedStatistic numeratorStat, ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += remoteContainer.getValue(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = remoteContainer.getValue(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double averageLocalAndRemote(ExtendedStatistic numeratorStat, ExtendedStatistic... denominatorStats) throws ExtendedStatisticNotFoundException {
      double denominator = 0;
      for (ExtendedStatistic denominatorStat : denominatorStats) {
         denominator += remoteContainer.getValue(denominatorStat);
         denominator += localContainer.getValue(denominatorStat);
      }
      if (denominator != 0) {
         double numerator = remoteContainer.getValue(numeratorStat);
         numerator += localContainer.getValue(numeratorStat);
         return numerator / denominator;
      }
      return 0;
   }

   private double microAverageLocal(ExtendedStatistic numerator, ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageLocal(numerator, denominator));
   }

   private double microAverageRemote(ExtendedStatistic numerator, ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageRemote(numerator, denominator));
   }

   private double microAverageLocalAndRemote(ExtendedStatistic numerator, ExtendedStatistic denominator) throws ExtendedStatisticNotFoundException {
      return convertNanosToMicro(averageLocalAndRemote(numerator, denominator));
   }

}
