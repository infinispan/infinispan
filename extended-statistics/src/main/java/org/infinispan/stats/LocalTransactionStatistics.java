package org.infinispan.stats;

import org.infinispan.stats.container.LocalExtendedStatisticsContainer;
import org.infinispan.util.TimeService;

import static org.infinispan.stats.container.ExtendedStatistic.*;

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
         copyValue(NUM_GET, NUM_GETS_RO_TX);
         copyValue(NUM_REMOTE_GET, NUM_REMOTE_GETS_RO_TX);
      } else {
         copyValue(NUM_GET, NUM_GETS_WR_TX);
         copyValue(NUM_REMOTE_GET, NUM_REMOTE_GETS_WR_TX);
         copyValue(NUM_PUT, NUM_PUTS_WR_TX);
         copyValue(NUM_REMOTE_PUT, NUM_REMOTE_PUTS_WR_TX);
         if (isCommitted()) {
            copyValue(NUM_HELD_LOCKS, NUM_HELD_LOCKS_SUCCESS_LOCAL_TX);
            if (optimisticLockingScheme) {
               copyValue(WR_TX_SUCCESSFUL_EXECUTION_TIME, LOCAL_EXEC_NO_CONT);
            } else {
               try {
                  double localLockAcquisitionTime = getValue(LOCK_WAITING_TIME);
                  double totalLocalDuration = getValue(WR_TX_SUCCESSFUL_EXECUTION_TIME);
                  this.addValue(LOCAL_EXEC_NO_CONT, (totalLocalDuration - localLockAcquisitionTime));
               } catch (ExtendedStatisticNotFoundException e) {
                  log.unableToCalculateLocalExecutionTimeWithoutContention(e);
               }
            }
         }
      }

   }
}
