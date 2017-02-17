package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An {@link Action} that checks if the command is the next to be executed.
 * <p>
 * This action is used by the triangle algorithm to order updates from the primary owner to the backup owner.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleOrderAction implements Action {

   private static final Log log = LogFactory.getLog(TriangleOrderAction.class);
   private static final boolean trace = log.isTraceEnabled();
   private final TriangleOrderManager triangleOrderManager;
   private final BlockingTaskAwareExecutorService remoteExecutorService;
   private final ClusteringDependentLogic clusteringDependentLogic;
   private final Object key;
   private final long sequenceNumber;
   private volatile int segmentId = -1;

   public TriangleOrderAction(TriangleOrderManager triangleOrderManager,
         BlockingTaskAwareExecutorService remoteExecutorService, ClusteringDependentLogic clusteringDependentLogic,
         long sequenceNumber, Object key) {
      this.triangleOrderManager = triangleOrderManager;
      this.remoteExecutorService = remoteExecutorService;
      this.clusteringDependentLogic = clusteringDependentLogic;
      this.sequenceNumber = sequenceNumber;
      this.key = key;
   }

   @Override
   public ActionStatus check(ActionState state) {
      int localSegmentId = computeSegmentIdIfNeeded();
      if (trace) {
         log.tracef("Checking if next for segment %s and sequence %s", localSegmentId, sequenceNumber);
      }
      return triangleOrderManager.isNext(localSegmentId, sequenceNumber, state.getCommandTopologyId()) ?
            ActionStatus.READY :
            ActionStatus.NOT_READY;
   }

   @Override
   public void onFinally(ActionState state) {
      triangleOrderManager.markDelivered(computeSegmentIdIfNeeded(), sequenceNumber, state.getCommandTopologyId());
      remoteExecutorService.checkForReadyTasks();
   }

   private int computeSegmentIdIfNeeded() {
      int tmp = segmentId;
      if (tmp == -1) {
         tmp = clusteringDependentLogic.getSegmentForKey(key);
         segmentId = tmp;
      }
      return tmp;
   }
}
