package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * Class to be extended to allow some control over the {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler}
 * in tests.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public abstract class AbstractDelegatingHandler implements PerCacheInboundInvocationHandler {

   protected final PerCacheInboundInvocationHandler delegate;

   protected AbstractDelegatingHandler(PerCacheInboundInvocationHandler delegate) {
      this.delegate = delegate;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      boolean canDelegate = beforeHandle(command, reply, order);
      if (canDelegate) {
         delegate.handle(command, reply, order);
      }
      afterHandle(command, order, canDelegate);
   }

   @Override
   public void setFirstTopologyAsMember(int firstTopologyAsMember) {
      delegate.setFirstTopologyAsMember(firstTopologyAsMember);
   }

   @Override
   public int getFirstTopologyAsMember() {
      return delegate.getFirstTopologyAsMember();
   }

   /**
    * Invoked before the command is handled by the real {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler}.
    *
    * @return {@code true} if the command should be handled by the read {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler},
    * {@code false} otherwise.
    */
   protected boolean beforeHandle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      return true;
   }

   /**
    * Invoked after the command is handled.
    *
    * @param delegated {@code true} if the command was handled by the real {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler},
    *                  {@code false} otherwise.
    */
   protected void afterHandle(CacheRpcCommand command, DeliverOrder order, boolean delegated) {
      //no-op by default
   }


}
