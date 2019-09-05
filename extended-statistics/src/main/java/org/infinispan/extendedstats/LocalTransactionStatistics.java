package org.infinispan.extendedstats;

import org.infinispan.commons.time.TimeService;
import org.infinispan.extendedstats.container.ExtendedStatistic;
import org.infinispan.extendedstats.container.LocalExtendedStatisticsContainer;

/**
 * Represents the statistics collected for a local transaction
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
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
   }

   @Override
   public final boolean isLocalTransaction() {
      return true;
   }

   @Override
   protected final void terminate() {
      if (isReadOnly()) {
         copyValue(ExtendedStatistic.NUM_GET, ExtendedStatistic.NUM_GETS_RO_TX);
         copyValue(ExtendedStatistic.NUM_REMOTE_GET, ExtendedStatistic.NUM_REMOTE_GETS_RO_TX);
      } else {
         copyValue(ExtendedStatistic.NUM_GET, ExtendedStatistic.NUM_GETS_WR_TX);
         copyValue(ExtendedStatistic.NUM_REMOTE_GET, ExtendedStatistic.NUM_REMOTE_GETS_WR_TX);
         copyValue(ExtendedStatistic.NUM_PUT, ExtendedStatistic.NUM_PUTS_WR_TX);
         copyValue(ExtendedStatistic.NUM_REMOTE_PUT, ExtendedStatistic.NUM_REMOTE_PUTS_WR_TX);
         if (isCommitted()) {
            copyValue(ExtendedStatistic.NUM_HELD_LOCKS, ExtendedStatistic.NUM_HELD_LOCKS_SUCCESS_LOCAL_TX);
            if (optimisticLockingScheme) {
               copyValue(ExtendedStatistic.WR_TX_SUCCESSFUL_EXECUTION_TIME, ExtendedStatistic.LOCAL_EXEC_NO_CONT);
            } else {
               try {
                  double localLockAcquisitionTime = getValue(ExtendedStatistic.LOCK_WAITING_TIME);
                  double totalLocalDuration = getValue(ExtendedStatistic.WR_TX_SUCCESSFUL_EXECUTION_TIME);
                  this.addValue(ExtendedStatistic.LOCAL_EXEC_NO_CONT, (totalLocalDuration - localLockAcquisitionTime));
               } catch (ExtendedStatisticNotFoundException e) {
                  log.unableToCalculateLocalExecutionTimeWithoutContention(e);
               }
            }
         }
      }

   }
}
