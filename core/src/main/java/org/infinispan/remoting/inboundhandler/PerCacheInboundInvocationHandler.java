package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * Interface to invoke when a {@link CacheRpcCommand} is received from other node in the
 * local site.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public interface PerCacheInboundInvocationHandler {

   /**
    * Handles the {@link CacheRpcCommand} from other node.
    *
    * @param command the {@link CacheRpcCommand} to handle.
    * @param reply   the return value is passed to this object in order to be sent back to the sender
    * @param order   the {@link DeliverOrder} in which the command was sent
    */
   void handle(CacheRpcCommand command, Reply reply, DeliverOrder order);


   /**
    * @param firstTopologyAsMember The first topology in which the local node was a member.
    *                              Any command with a lower topology id will be ignored.
    */
   void setFirstTopologyAsMember(int firstTopologyAsMember);

   /**
    * @return The first topology in which the local node was a member.
    *
    * Any command with a lower topology id will be ignored.
    */
   int getFirstTopologyAsMember();

   /**
    * Checks if any pending tasks are now ready to be ran and will run in them in a separate thread. This method does not
    * block.
    */
   void checkForReadyTasks();
}
