package org.infinispan.stats;

import org.infinispan.stats.container.ConcurrentGlobalContainer;
import org.infinispan.stats.container.ExtendedStatistic;
import org.infinispan.stats.container.ExtendedStatisticsContainer;
import org.infinispan.stats.logging.Log;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.LogFactory;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;


/**
 * Keeps the temporary statistics for a transaction. Also, it has the common logic for the local and remote
 * transactions
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class TransactionStatistics {

   //Here the elements which are common for local and remote transactions
   protected final long initTime;
   protected final Log log = LogFactory.getLog(getClass(), Log.class);
   protected final TimeService timeService;
   private final ExtendedStatisticsContainer container;
   private boolean readOnly;
   private boolean committed;


   protected TransactionStatistics(ExtendedStatisticsContainer container, TimeService timeService) {
      this.timeService = timeService;
      this.initTime = timeService.time();
      this.readOnly = true; //as far as it does not tries to perform a put operation
      this.container = container;
      if (log.isTraceEnabled()) {
         log.tracef("Created transaction statistics. Start time=%s", initTime);
      }
   }

   /**
    * @return {@code true} if the transaction committed successfully, {@code false} otherwise
    */
   public final boolean isCommitted() {
      return this.committed;
   }

   /**
    * Sets the transaction outcome. See {@link #isCommitted()}.
    *
    * @param commit {@code true} if the transaction is committed successfully.
    */
   public final void setOutcome(boolean commit) {
      committed = commit;
   }

   /**
    * @return {@code true} if this transaction is a read-only transaction.
    */
   public final boolean isReadOnly() {
      return this.readOnly;
   }

   /**
    * Sets this transaction as a write transaction. See also {@link #isReadOnly()}.
    */
   public final void markAsUpdateTransaction() {
      this.readOnly = false;
   }

   /**
    * Adds a value to a statistic collected for this transaction.
    */
   public final void addValue(ExtendedStatistic stat, double value) {
      container.addValue(stat, value);
      if (log.isTraceEnabled()) {
         log.tracef("Add %s to %s", value, stat);
      }
   }

   /**
    * @return a value collected for this transaction.
    * @throws ExtendedStatisticNotFoundException
    *          if the statistic collected was not found.
    */
   public final double getValue(ExtendedStatistic stat) throws ExtendedStatisticNotFoundException {
      double value = container.getValue(stat);
      if (log.isTraceEnabled()) {
         log.tracef("Value of %s is %s", stat, value);
      }
      return value;
   }

   /**
    * Increments a statistic value. It is equivalent to {@code addValue(stat, 1)}.
    */
   public final void incrementValue(ExtendedStatistic stat) {
      this.addValue(stat, 1);
   }

   /**
    * Signals this transaction as completed and updates the statistics to the final values ready to be merged in the
    * cache statistics.
    */
   public final void terminateTransaction() {
      if (log.isTraceEnabled()) {
         log.tracef("Terminating transaction. Is read only? %s. Is commit? %s", readOnly, committed);
      }
      long execTime = timeService.timeDuration(initTime, NANOSECONDS);
      if (readOnly) {
         if (committed) {
            incrementValue(NUM_COMMITTED_RO_TX);
            addValue(RO_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
         } else {
            incrementValue(NUM_ABORTED_RO_TX);
            addValue(RO_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      } else {
         if (committed) {
            incrementValue(NUM_COMMITTED_WR_TX);
            addValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, execTime);
         } else {
            incrementValue(NUM_ABORTED_WR_TX);
            addValue(WR_TX_ABORTED_EXECUTION_TIME, execTime);
         }
      }

      terminate();
   }

   /**
    * Merges this statistics in the global container.
    */
   public final void flushTo(ConcurrentGlobalContainer globalContainer) {
      if (log.isTraceEnabled()) {
         log.tracef("Flush this [%s] to %s", this, globalContainer);
      }
      container.mergeTo(globalContainer);
   }

   @Override
   public String toString() {
      return "initTime=" + initTime +
            ", readOnly=" + readOnly +
            ", committed=" + committed +
            '}';
   }

   /**
    * Signals the reception of the {@link org.infinispan.commands.tx.PrepareCommand}.
    */
   public abstract void onPrepareCommand();

   /**
    * @return {@code true} if this transaction statistics is for a local transaction.
    */
   public abstract boolean isLocalTransaction();

   /**
    * Signals this transaction as completed and updates the statistics to the final values ready to be merged in the
    * cache statistics. This method is abstract in order to be override for the local and the remote transactions.
    */
   protected abstract void terminate();

   /**
    * Copies a statistic value and adds it to another statistic.
    */
   protected final void copyValue(ExtendedStatistic from, ExtendedStatistic to) {
      try {
         double value = container.getValue(from);
         container.addValue(to, value);
         if (log.isDebugEnabled()) {
            log.debugf("Copy value [%s] from [%s] to [%s]", value, from, to);
         }
      } catch (ExtendedStatisticNotFoundException e) {
         log.unableToCopyValue(from, to, e);
      }
   }
}

