package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.remoting.inboundhandler.TrianglePerCacheInboundInvocationHandler;
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
   private final long sequenceNumber;
   private final TrianglePerCacheInboundInvocationHandler handler;
   private final int segmentId;

   public TriangleOrderAction(TrianglePerCacheInboundInvocationHandler handler, long sequenceNumber, int segmentId) {
      this.handler = handler;
      this.sequenceNumber = sequenceNumber;
      this.segmentId = segmentId;
   }

   @Override
   public ActionStatus check(ActionState state) {
      if (trace) {
         log.tracef("Checking if next for segment %s and sequence %s", segmentId, sequenceNumber);
      }
      return handler.getTriangleOrderManager().isNext(segmentId, sequenceNumber, state.getCommandTopologyId()) ?
            ActionStatus.READY :
            ActionStatus.NOT_READY;
   }

   @Override
   public void onFinally(ActionState state) {
      handler.getTriangleOrderManager().markDelivered(segmentId, sequenceNumber, state.getCommandTopologyId());
      handler.getRemoteExecutor().checkForReadyTasks();
   }
}
