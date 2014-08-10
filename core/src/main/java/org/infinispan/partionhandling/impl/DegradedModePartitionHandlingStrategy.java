package org.infinispan.partionhandling.impl;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DegradedModePartitionHandlingStrategy implements PartitionHandlingStrategy {

   private static final Log log = LogFactory.getLog(DegradedModePartitionHandlingStrategy.class);

   @Override
   public void onMembershipChanged(PartitionContext pc) {
      if (!pc.isMissingData()) {
         log.debug("No partition, proceeding to rebalance.");
         pc.rebalance();
         return;
      }
      pc.enterDegradedMode();
   }
}
