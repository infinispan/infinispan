package org.infinispan.stats;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.logging.Log;
import org.infinispan.stats.percentiles.PercentileStatistic;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages all the statistics for a single cache. All the statistics should be added in this class.
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public final class CacheStatisticManager {

   private static final Log log = LogFactory.getLog(CacheStatisticManager.class, Log.class);
   /**
    * collects statistic for a remote transaction
    */
   private final ConcurrentMap<GlobalTransaction, RemoteTransactionStatistics> remoteTransactionStatistics =
         CollectionFactory.makeConcurrentMap();
   /**
    * collects statistic for a local transaction
    */
   private final ConcurrentMap<GlobalTransaction, LocalTransactionStatistics> localTransactionStatistics =
         CollectionFactory.makeConcurrentMap();
   /**
    * collects statistic for a cache
    */
   private final CacheStatisticCollector cacheStatisticCollector;
   private final boolean isOptimisticLocking;
   private final TimeService timeService;

   public CacheStatisticManager(Configuration configuration, TimeService timeService) {
      this.timeService = timeService;
      this.isOptimisticLocking = configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC;
      this.cacheStatisticCollector = new CacheStatisticCollector(timeService);
   }

   /**
    * Adds a value to a statistic.
    *
    * @param globalTransaction the global transaction in which the statistics was updated
    * @param local             {@code true} if measurement occurred in a local context.
    */
   public final void add(ExtendedStatistic stat, double value, GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to add %s to %s but no transaction exist. Add to Cache Statistic", value, stat);
         }
         if (local) {
            cacheStatisticCollector.addLocalValue(stat, value);
         } else {
            cacheStatisticCollector.addRemoteValue(stat, value);
         }
         return;
      }
      txs.addValue(stat, value);
   }

   /**
    * Increments the statistic value. It is equivalent to {@code add(stat, 1, globalTransaction, local)}.
    *
    * @param globalTransaction the global transaction in which the statistics was updated
    * @param local             {@code true} if measurement occurred in a local context.
    */
   public final void increment(ExtendedStatistic stat, GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         if (log.isDebugEnabled()) {
            log.debugf("Trying to increment %s but no transaction exist. Add to Cache Statistic", stat);
         }
         if (local) {
            cacheStatisticCollector.addLocalValue(stat, 1);
         } else {
            cacheStatisticCollector.addRemoteValue(stat, 1);
         }
         return;
      }
      txs.addValue(stat, 1);
   }

   /**
    * Invoked when a {@link org.infinispan.commands.tx.PrepareCommand} is received for a transaction.
    *
    * @param globalTransaction the global transaction to be prepared.
    * @param local             {@code true} if measurement occurred in a local context.
    */
   public final void onPrepareCommand(GlobalTransaction globalTransaction, boolean local) {
      //NB: If I want to give up using the InboundInvocationHandler, I can create the remote transaction
      //here, just overriding the handlePrepareCommand
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         log.prepareOnUnexistingTransaction(globalTransaction == null ? "null" : globalTransaction.globalId());
         log.error("Trying to invoke onPrepareCommand() but no transaction is associated");
         return;
      }
      txs.onPrepareCommand();
   }

   /**
    * Sets the transaction outcome to commit or rollback, depending if the transaction has commit successfully or not
    * respectively.
    *
    * @param commit            {@code true} if the transaction has committed successfully.
    * @param globalTransaction the terminated global transaction.
    * @param local             {@code true} if measurement occurred in a local context.
    */
   public final void setTransactionOutcome(boolean commit, GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         log.outcomeOnUnexistingTransaction(globalTransaction == null ? "null" : globalTransaction.globalId(),
                                            commit ? "COMMIT" : "ROLLBACK");
         return;
      }
      txs.setOutcome(commit);
   }

   /**
    * @return the current value of the statistic. If the statistic is not exported (via JMX), then the sum of the remote
    *         and local value is returned.
    * @throws ExtendedStatisticNotFoundException
    *          if the statistic is not found.
    */
   public final double getAttribute(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      return cacheStatisticCollector.getAttribute(stat);
   }

   /**
    * @return the percentile og the statistic.
    * @throws IllegalArgumentException if the percentile request is not in the correct bounds (]0,100[)
    */
   public final double getPercentile(PercentileStatistic stat, int percentile) throws IllegalArgumentException {
      return cacheStatisticCollector.getPercentile(stat, percentile);
   }

   /**
    * Marks the transaction as a write transaction (instead of a read only transaction)
    *
    * @param local {@code true} if it is a local transaction.
    */
   public final void markAsWriteTransaction(GlobalTransaction globalTransaction, boolean local) {
      TransactionStatistics txs = getTransactionStatistic(globalTransaction, local);
      if (txs == null) {
         log.markUnexistingTransactionAsWriteTransaction(globalTransaction == null ? "null" : globalTransaction.globalId());
         return;
      }
      txs.markAsUpdateTransaction();
   }

   /**
    * Signals the start of a transaction.
    *
    * @param local {@code true} if the transaction is local.
    */
   public final void beginTransaction(GlobalTransaction globalTransaction, boolean local) {
      if (local) {
         //Not overriding the InitialValue method leads me to have "null" at the first invocation of get()
         TransactionStatistics lts = createTransactionStatisticIfAbsent(globalTransaction, true);
         if (log.isTraceEnabled()) {
            log.tracef("Local transaction statistic is already initialized: %s", lts);
         }
      } else {
         TransactionStatistics rts = createTransactionStatisticIfAbsent(globalTransaction, false);
         if (log.isTraceEnabled()) {
            log.tracef("Using the remote transaction statistic %s for transaction %s", rts, globalTransaction);
         }
      }
   }

   /**
    * Signals the ending of a transaction. After this, no more statistics are updated for this transaction and the
    * values measured are merged with the cache statistics.
    *
    * @param local  {@code true} if the transaction is local.
    * @param remote {@code true} if the transaction is remote.
    */
   public final void terminateTransaction(GlobalTransaction globalTransaction, boolean local, boolean remote) {
      TransactionStatistics txs = null;

      if (local) {
         txs = removeTransactionStatistic(globalTransaction, true);
      }
      if (txs != null) {
         txs.terminateTransaction();
         cacheStatisticCollector.merge(txs);
         txs = null;
      }
      if (remote) {
         txs = removeTransactionStatistic(globalTransaction, false);
      }
      if (txs != null) {
         txs.terminateTransaction();
         cacheStatisticCollector.merge(txs);
      }
   }

   /**
    * Resets the cache statistics collected so far.
    */
   public final void reset() {
      cacheStatisticCollector.reset();
   }

   /**
    * @return {@code true} if it has some transaction pending, i.e., transaction in which the statistics can be
    *         updated.
    */
   public final boolean hasPendingTransactions() {
      log.debugf("Checking for pending transactions. local=%s, remote=%s", localTransactionStatistics.keySet(),
                 remoteTransactionStatistics.keySet());
      return !localTransactionStatistics.isEmpty() || !remoteTransactionStatistics.isEmpty();
   }

   /**
    * @return a String with all the cache statistic values.
    */
   public final String dumpCacheStatistics() {
      StringBuilder builder = new StringBuilder();
      cacheStatisticCollector.dumpTo(builder);
      return builder.toString();
   }

   /**
    * Prints the cache statistics values to a {@link PrintStream}.
    */
   public final void dumpCacheStatisticsTo(PrintStream stream) {
      cacheStatisticCollector.dumpTo(new PrintWriter(stream));
   }

   private TransactionStatistics getTransactionStatistic(GlobalTransaction globalTransaction, boolean local) {
      if (globalTransaction != null) {
         return local ? localTransactionStatistics.get(globalTransaction) :
               remoteTransactionStatistics.get(globalTransaction);
      }
      return null;
   }

   private TransactionStatistics removeTransactionStatistic(GlobalTransaction globalTransaction, boolean local) {
      if (globalTransaction != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Removing %s statistic for %s", local ? "local" : "remote", globalTransaction.globalId());
         }
         return local ? localTransactionStatistics.remove(globalTransaction) :
               remoteTransactionStatistics.remove(globalTransaction);
      }
      return null;
   }

   private TransactionStatistics createTransactionStatisticIfAbsent(GlobalTransaction globalTransaction, boolean local) {
      if (globalTransaction == null) {
         throw new NullPointerException("Global Transaction cannot be null");
      }
      TransactionStatistics ts = local ? localTransactionStatistics.get(globalTransaction) :
            remoteTransactionStatistics.get(globalTransaction);

      if (ts != null) {
         return ts;
      }

      if (local) {
         if (log.isTraceEnabled()) {
            log.tracef("Creating local statistic for %s", globalTransaction.globalId());
         }
         LocalTransactionStatistics lts = new LocalTransactionStatistics(isOptimisticLocking, timeService);
         TransactionStatistics existing = localTransactionStatistics.putIfAbsent(globalTransaction, lts);
         return existing == null ? lts : existing;
      } else {
         if (log.isTraceEnabled()) {
            log.tracef("Creating remote statistic for %s", globalTransaction.globalId());
         }
         RemoteTransactionStatistics rts = new RemoteTransactionStatistics(timeService);
         TransactionStatistics existing = remoteTransactionStatistics.putIfAbsent(globalTransaction, rts);
         return existing == null ? rts : existing;
      }
   }

}
