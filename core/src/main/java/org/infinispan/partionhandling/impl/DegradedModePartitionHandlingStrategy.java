package org.infinispan.partionhandling.impl;

import org.infinispan.partionhandling.MergeContext;
import org.infinispan.partionhandling.PartitionContext;
import org.infinispan.partionhandling.PartitionHandlingStrategy;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class DegradedModePartitionHandlingStrategy implements PartitionHandlingStrategy {

   private static Log log = LogFactory.getLog(DegradedModePartitionHandlingStrategy.class);

   @Override
   public void onPartition(PartitionContext pc) {
      if (!pc.isPartition()) {
         log.debug("No partition, proceeding to rebalance.");
         pc.rebalance();
         return;
      }
      pc.currentPartitionDegradedMode();
   }
}
