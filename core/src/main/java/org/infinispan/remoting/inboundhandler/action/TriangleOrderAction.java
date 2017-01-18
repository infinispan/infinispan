package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.distribution.TriangleOrderManager;
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
   private final int segmentId;
   private final long sequenceNumber;

   public TriangleOrderAction(TriangleOrderManager triangleOrderManager,
         BlockingTaskAwareExecutorService remoteExecutorService, int segmentId, long sequenceNumber) {
      this.triangleOrderManager = triangleOrderManager;
      this.remoteExecutorService = remoteExecutorService;
      this.segmentId = segmentId;
      this.sequenceNumber = sequenceNumber;
   }

   @Override
   public ActionStatus check(ActionState state) {
      if (trace) {
         log.tracef("Checking if next for segment %s and sequence %s", segmentId, sequenceNumber);
      }
      return triangleOrderManager.isNext(segmentId, sequenceNumber, state.getCommandTopologyId()) ?
            ActionStatus.READY :
            ActionStatus.NOT_READY;
   }

   @Override
   public void onFinally(ActionState state) {
      triangleOrderManager.markDelivered(segmentId, sequenceNumber, state.getCommandTopologyId());
      remoteExecutorService.checkForReadyTasks();
   }
}
