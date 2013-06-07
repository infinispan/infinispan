package org.infinispan.stats;

import org.infinispan.stats.container.LocalExtendedStatisticsContainer;
import org.infinispan.stats.exception.ExtendedStatisticNotFoundException;
import org.infinispan.util.TimeService;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.infinispan.stats.container.ExtendedStatistic.*;

/**
 * Represents the statistics collected for a local transaction
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 5.3
 */
public class LocalTransactionStatistics extends TransactionStatistics {

   private final boolean optimisticLockingScheme;
   private boolean stillLocalExecution;

   public LocalTransactionStatistics(boolean optimisticLockingScheme, TimeService timeService) {
      super(new LocalExtendedStatisticsContainer(), timeService);
      this.optimisticLockingScheme = optimisticLockingScheme;
      this.stillLocalExecution = true;
   }

   @Override
   public final String toString() {
      return "LocalTransactionStatistics{" +
            "stillLocalExecution=" + stillLocalExecution +
            ", " + super.toString();
   }

   @Override
   public final void onPrepareCommand() {
      this.stillLocalExecution = false;

      if (!isReadOnly()) {
         this.addValue(WR_TX_LOCAL_EXECUTION_TIME, timeService.timeDuration(initTime, NANOSECONDS));
      }
      this.incrementValue(NUM_PREPARES);
   }

   @Override
   public final boolean isLocalTransaction() {
      return true;
   }

   @Override
   protected final void terminate() {
      if (!isReadOnly() && isCommitted()) {
         copyValue(NUM_PUT, NUM_SUCCESSFUL_PUTS);
         copyValue(NUM_HELD_LOCKS, NUM_HELD_LOCKS_SUCCESS_TX);
         if (optimisticLockingScheme) {
            copyValue(WR_TX_LOCAL_EXECUTION_TIME, LOCAL_EXEC_NO_CONT);
         } else {
            try {
               double localLockAcquisitionTime = getValue(LOCK_WAITING_TIME);
               double totalLocalDuration = getValue(WR_TX_LOCAL_EXECUTION_TIME);
               this.addValue(LOCAL_EXEC_NO_CONT, (totalLocalDuration - localLockAcquisitionTime));
            } catch (ExtendedStatisticNotFoundException e) {
               log.warnf("Cannot calculate local execution time without contention. %s", e.getLocalizedMessage());
            }
         }
      }
   }
}
