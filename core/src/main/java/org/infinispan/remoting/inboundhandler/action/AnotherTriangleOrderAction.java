package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.distribution.CommandPosition;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public class AnotherTriangleOrderAction implements Action {

   private static final Log log = LogFactory.getLog(AnotherTriangleOrderAction.class);
   private static final boolean trace = log.isTraceEnabled();
   private final BlockingTaskAwareExecutorService remoteExecutorService;
   private volatile CommandPosition position;

   public AnotherTriangleOrderAction(BlockingTaskAwareExecutorService remoteExecutorService, CommandPosition position) {
      this.remoteExecutorService = remoteExecutorService;
      this.position = position;
   }

   @Override
   public ActionStatus check(ActionState state) {
      return position.isNext() ?
            ActionStatus.READY :
            ActionStatus.NOT_READY;
   }

   @Override
   public void onFinally(ActionState state) {
      position.finish();
      remoteExecutorService.checkForReadyTasks();
   }
}
