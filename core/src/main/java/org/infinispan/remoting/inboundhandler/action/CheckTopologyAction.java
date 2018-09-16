package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.remoting.inboundhandler.BasePerCacheInboundInvocationHandler;

/**
 * An {@link Action} implementation that checks if the command topology id is valid.
 * <p>
 * The command topology id is valid when it is higher or equal thant the first topology as member for this node.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class CheckTopologyAction implements Action {

   private final BasePerCacheInboundInvocationHandler handler;

   public CheckTopologyAction(BasePerCacheInboundInvocationHandler handler) {
      this.handler = handler;
   }

   @Override
   public ActionStatus check(ActionState state) {
      return handler.isCommandSentBeforeFirstTopology(state.getCommandTopologyId()) ?
            ActionStatus.CANCELED :
            ActionStatus.READY;
   }
}
