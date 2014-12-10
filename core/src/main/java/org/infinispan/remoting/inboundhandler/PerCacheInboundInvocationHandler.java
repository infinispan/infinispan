package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * Interface to invoke when a {@link org.infinispan.commands.remote.CacheRpcCommand} is received from other node in the
 * local site.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public interface PerCacheInboundInvocationHandler {

   /**
    * Handles the {@link org.infinispan.commands.remote.CacheRpcCommand} from other node.
    *
    * @param command the {@link org.infinispan.commands.remote.CacheRpcCommand} to handle-
    * @param reply   the return value is passed to this object in order to be sent back to the sender
    * @param order   the {@link org.infinispan.remoting.inboundhandler.DeliverOrder} in which the command was sent
    */
   void handle(CacheRpcCommand command, Reply reply, DeliverOrder order);

}
