package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;

/**
 * An {@link Action} that checks if the command is the next to be executed.
 * <p>
 * This action is used by the triangle algorithm to order updates from the primary owner to the backup owner.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleOrderAction implements Action {

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
