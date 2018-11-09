package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.jmx.JmxStatisticsExposer;

/**
 * Interface to invoke when a {@link org.infinispan.commands.remote.CacheRpcCommand} is received from other node in the
 * local site.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public interface PerCacheInboundInvocationHandler extends JmxStatisticsExposer {

   /**
    * Handles the {@link org.infinispan.commands.remote.CacheRpcCommand} from other node.
    *
    * @param command the {@link org.infinispan.commands.remote.CacheRpcCommand} to handle-
    * @param reply   the return value is passed to this object in order to be sent back to the sender
    * @param order   the {@link org.infinispan.remoting.inboundhandler.DeliverOrder} in which the command was sent
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

   void registerXSiteCommandReceiver(boolean sync);
}
