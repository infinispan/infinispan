package org.infinispan.remoting.rpc;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;

/**
 * Provides a mechanism for communicating with other caches in the cluster, by formatting and passing requests down to
 * the registered {@link Transport}.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface RpcManager {

   /**
    * Invokes a command on remote nodes.
    *
    * @param recipients A list of nodes, or {@code null} to invoke the command on all the members of the cluster
    * @param rpc The command to invoke
    * @param options The invocation options
    * @return A future that, when completed, returns the responses from the remote nodes.
    */
   CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                 ReplicableCommand rpc, RpcOptions options);

   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients a list of Addresses to invoke the call on.  If this is {@code null}, the call is broadcast to the
    *                   entire cluster.
    * @param rpc        command to execute remotely.
    * @param options    it configures the invocation. The same instance can be re-used since {@link RpcManager} does
    *                   not change it. Any change in {@link RpcOptions} during a remote invocation can lead to
    *                   unpredictable behavior.
    * @return  a map of responses from each member contacted.
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, RpcOptions options);

   Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcs, RpcOptions options);

   /**
    * Asynchronously sends the {@link ReplicableCommand} to the destination using the specified {@link DeliverOrder}.
    *
    * @param destination  the destination's {@link Address}.
    * @param command      the {@link ReplicableCommand} to send.
    * @param deliverOrder the {@link DeliverOrder} to use.
    */
   void sendTo(Address destination, ReplicableCommand command, DeliverOrder deliverOrder);

   /**
    * Asynchronously sends the {@link ReplicableCommand} to the set of destination using the specified {@link
    * DeliverOrder}.
    *
    * @param destinations the collection of destination's {@link Address}. If {@code null}, it sends to all the members
    *                     in the cluster.
    * @param command      the {@link ReplicableCommand} to send.
    * @param deliverOrder the {@link DeliverOrder} to use.
    */
   void sendToMany(Collection<Address> destinations, ReplicableCommand command, DeliverOrder deliverOrder);

   /**
    * @return a reference to the underlying transport.
    */
   Transport getTransport();


   /**
    * Returns members of a cluster scoped to the cache owning this RpcManager. Note that this List
    * is always a subset of {@link Transport#getMembers()}
    *
    * @return a list of cache scoped cluster members
    */
   List<Address> getMembers();

   /**
    * Returns the address associated with this RpcManager or null if not part of the cluster.
    */
   Address getAddress();

   /**
    * Returns the current topology id. As opposed to the viewId which is updated whenever the cluster changes,
    * the topologyId is updated when a new cache instance is started or removed - this doesn't necessarily coincide
    * with a node being added/removed to the cluster.
    */
   int getTopologyId();

   /**
    * Creates a new {@link org.infinispan.remoting.rpc.RpcOptionsBuilder}.
    * <p/>
    * The {@link org.infinispan.remoting.rpc.RpcOptionsBuilder} is configured with the {@link org.infinispan.remoting.rpc.ResponseMode} and with
    * {@link org.infinispan.remoting.inboundhandler.DeliverOrder#NONE} if the {@link
    * org.infinispan.remoting.rpc.ResponseMode} is synchronous otherwise, with {@link
    * org.infinispan.remoting.inboundhandler.DeliverOrder#PER_SENDER} if asynchronous.
    *
    * @param responseMode the {@link org.infinispan.remoting.rpc.ResponseMode}.
    * @return a new {@link RpcOptionsBuilder} with the default options. The response and deliver mode are set as
    * described.
    */
   RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode);

   /**
    * Creates a new {@link org.infinispan.remoting.rpc.RpcOptionsBuilder}.
    *
    * @param responseMode the {@link org.infinispan.remoting.rpc.ResponseMode}.
    * @param deliverOrder  the {@link org.infinispan.remoting.inboundhandler.DeliverOrder}.
    * @return a new {@link RpcOptionsBuilder} with the default options and the response mode and deliver mode set by the
    * parameters.
    */
   RpcOptionsBuilder getRpcOptionsBuilder(ResponseMode responseMode, DeliverOrder deliverOrder);

   /**
    * Creates a new {@link org.infinispan.remoting.rpc.RpcOptionsBuilder}.
    * <p/>
    * The {@link org.infinispan.remoting.rpc.RpcOptionsBuilder} is configured with {@link
    * org.infinispan.remoting.inboundhandler.DeliverOrder#NONE} if the {@param sync} is {@code true} otherwise, with
    * {@link org.infinispan.remoting.inboundhandler.DeliverOrder#PER_SENDER}.
    *
    * @param sync {@code true} for Synchronous RpcOptions
    * @return the default Synchronous/Asynchronous RpcOptions
    */
   RpcOptions getDefaultRpcOptions(boolean sync);

   /**
    * Creates a new {@link org.infinispan.remoting.rpc.RpcOptionsBuilder}.
    *
    * @param sync        {@code true} for Synchronous RpcOptions
    * @param deliverOrder the {@link org.infinispan.remoting.inboundhandler.DeliverOrder} to use.
    * @return the default Synchronous/Asynchronous RpcOptions with the deliver order set by the parameter.
    */
   RpcOptions getDefaultRpcOptions(boolean sync, DeliverOrder deliverOrder);
}
