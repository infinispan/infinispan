package org.infinispan.extendedstats;

import org.infinispan.commons.time.TimeService;
import org.infinispan.extendedstats.container.RemoteExtendedStatisticsContainer;

/**
 * Represents the statistic collected for a remote transaction
 *
 * @author Roberto Palmieri
 * @author Sebastiano Peluso
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class RemoteTransactionStatistics extends TransactionStatistics {

   public RemoteTransactionStatistics(TimeService timeService) {
      super(new RemoteExtendedStatisticsContainer(), timeService);
   }

   @Override
   public final String toString() {
      return "RemoteTransactionStatistics{" + super.toString();
   }

   @Override
   public final void onPrepareCommand() {
      //nop
   }

   @Override
   public final boolean isLocalTransaction() {
      return false;
   }

   @Override
   protected final void terminate() {
      //nop
   }
}
