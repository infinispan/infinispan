package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * Interface to invoke when the {@link org.infinispan.remoting.transport.Transport} receives a command from other node
 * or site.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public interface InboundInvocationHandler {

   /**
    * Handles the {@link org.infinispan.commands.ReplicableCommand} from other node belonging to local site.
    *
    * @param origin  the sender {@link org.infinispan.remoting.transport.Address}
    * @param command the {@link org.infinispan.commands.ReplicableCommand} to handler
    * @param reply   the return value is passed to this object in order to be sent back to the origin
    * @param order   the {@link org.infinispan.remoting.inboundhandler.DeliverOrder} in which the command was sent
    */
   void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order);

   /**
    * Handles the {@link org.infinispan.commands.ReplicableCommand} from remote site.
    * @param origin  the sender site
    * @param command the {@link org.infinispan.commands.ReplicableCommand} to handle
    * @param reply   the return value is passed to this object in order to be sent back to the origin
    * @param order   the {@link DeliverOrder} in which the command was sent
    */
   void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order);
}
