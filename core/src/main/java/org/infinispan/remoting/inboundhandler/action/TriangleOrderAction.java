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
   private final Object key;
   private final long sequenceNumber;
   private final TrianglePerCacheInboundInvocationHandler handler;
   private volatile int segmentId = -1;

   public TriangleOrderAction(TrianglePerCacheInboundInvocationHandler handler, long sequenceNumber, Object key) {
      this.handler = handler;
      this.sequenceNumber = sequenceNumber;
      this.key = key;
   }

   @Override
   public ActionStatus check(ActionState state) {
      int localSegmentId = computeSegmentIdIfNeeded();
      if (trace) {
         log.tracef("Checking if next for segment %s and sequence %s", localSegmentId, sequenceNumber);
      }
      return handler.getTriangleOrderManager().isNext(localSegmentId, sequenceNumber, state.getCommandTopologyId()) ?
            ActionStatus.READY :
            ActionStatus.NOT_READY;
   }

   @Override
   public void onFinally(ActionState state) {
      handler.getTriangleOrderManager().markDelivered(computeSegmentIdIfNeeded(), sequenceNumber, state.getCommandTopologyId());
      handler.getRemoteExecutor().checkForReadyTasks();
   }

   private int computeSegmentIdIfNeeded() {
      int tmp = segmentId;
      if (tmp == -1) {
         tmp = handler.getClusteringDependentLogic().getCacheTopology().getSegment(key);
         segmentId = tmp;
      }
      return tmp;
   }
}
