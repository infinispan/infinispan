package org.horizon.remoting.transport;

import org.horizon.commands.RPCCommand;
import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.lifecycle.Lifecycle;
import org.horizon.marshall.Marshaller;
import org.horizon.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.horizon.remoting.InboundInvocationHandler;
import org.horizon.remoting.ResponseFilter;
import org.horizon.remoting.ResponseMode;
import org.horizon.statetransfer.StateTransferException;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * An interface that provides a communication link with remote caches.  Also allows remote caches to invoke commands on
 * this cache instance.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@NonVolatile
public interface Transport extends Lifecycle {
   // TODO discovery should be abstracted away into a separate set of interfaces such that it is not tightly coupled to the transport

   /**
    * Initializes the transport with global cache configuration and transport-specific properties.
    *
    * @param c                      global cache-wide configuration
    * @param p                      properties to set
    * @param marshaller             marshaller to use for marshalling and unmarshalling
    * @param asyncExecutor          executor to use for asynchronous calls
    * @param handler                handler for invoking remotely originating calls on the local cache
    * @param notifier               notifier to use
    * @param distributedSyncTimeout timeout to wait for distributed syncs
    */
   void initialize(GlobalConfiguration c, Properties p, Marshaller marshaller, ExecutorService asyncExecutor,
                   InboundInvocationHandler handler, CacheManagerNotifier notifier, long distributedSyncTimeout);

   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients       a list of Addresses to invoke the call on.  If this is null, the call is broadcast to the
    *                         entire cluster.
    * @param rpcCommand       the cache command to invoke
    * @param mode             the response mode to use
    * @param timeout          a timeout after which to throw a replication exception.
    * @param usePriorityQueue if true, a priority queue is used to deliver messages.  May not be supported by all
    *                         implementations.
    * @param responseFilter   a response filter with which to filter out failed/unwanted/invalid responses.
    * @param supportReplay    whether replays of missed messages is supported
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Object> invokeRemotely(List<Address> recipients, RPCCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean supportReplay) throws Exception;

   /**
    * @return true if the current Channel is the coordinator of the cluster.
    */
   boolean isCoordinator();

   /**
    * @return the Address of the current coordinator.
    */
   Address getCoordinator();

   /**
    * Retrieves the current cache instance's network address
    *
    * @return an Address
    */
   Address getAddress();

   /**
    * Returns a list of  members in the current cluster view.
    *
    * @return a list of members.  Typically, this would be defensively copied.
    */
   List<Address> getMembers();

   /**
    * Initiates a state retrieval from a specific cache (by typically invoking {@link
    * org.horizon.remoting.InboundInvocationHandler#generateState(String, java.io.OutputStream)}), and applies this
    * state to the current cache via the  {@link InboundInvocationHandler#applyState(String, java.io.InputStream)}
    * callback.
    *
    * @param cacheName name of cache for which to retrieve state
    * @param address   address of remote cache from which to retrieve state
    * @param timeout   state retrieval timeout in milliseconds
    * @return true if state was transferred and applied successfully, false if it timed out.
    * @throws org.horizon.statetransfer.StateTransferException
    *          if state cannot be retrieved from the specific cache
    */
   boolean retrieveState(String cacheName, Address address, long timeout) throws StateTransferException;

   /**
    * @return an instance of a DistributedSync that can be used to wait for synchronization events across a cluster.
    */
   DistributedSync getDistributedSync();

   /**
    * Blocks all RPC calls to and between a set of Addresses.  If a null is passed in, the entire cluster is blocked.
    *
    * @param addresses addresses to block
    */
   void blockRPC(Address... addresses);

   /**
    * Releases a block performed by calling {@link #blockRPC(Address[])}
    */
   void unblockRPC();
}
