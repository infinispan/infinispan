package org.infinispan.remoting.transport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * An interface that provides a communication link with remote caches.  Also allows remote caches to invoke commands on
 * this cache instance.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface Transport extends Lifecycle {
   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients     a list of Addresses to invoke the call on.  If this is null, the call is broadcast to the
    *                       entire cluster.
    * @param rpcCommand     the cache command to invoke
    * @param mode           the response mode to use
    * @param timeout        a timeout after which to throw a replication exception. implementations.
    * @param responseFilter a response filter with which to filter out failed/unwanted/invalid responses.
    * @param deliverOrder   the {@link org.infinispan.remoting.inboundhandler.DeliverOrder}.
    * @param anycast        used when {@param totalOrder} is {@code true}, it means that it must use TOA instead of
    *                       TOB.
    * @return a map of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout,
                                         ResponseFilter responseFilter, DeliverOrder deliverOrder, boolean anycast) throws Exception;

   CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                 ReplicableCommand rpcCommand,
                                                                 ResponseMode mode, long timeout,
                                                                 ResponseFilter responseFilter,
                                                                 DeliverOrder deliverOrder,
                                                                 boolean anycast) throws Exception;

   /**
    * Asynchronously sends the {@link ReplicableCommand} to the destination using the specified {@link DeliverOrder}.
    *
    * @param destination  the destination's {@link Address}.
    * @param rpcCommand   the {@link ReplicableCommand} to send.
    * @param deliverOrder the {@link DeliverOrder} to use.
    * @throws Exception if there was problem sending the request.
    */
   void sendTo(Address destination, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception;

   /**
    * Asynchronously sends the {@link ReplicableCommand} to the set of destination using the specified {@link
    * DeliverOrder}.
    *
    * @param destinations the collection of destination's {@link Address}. If {@code null}, it sends to all the members
    *                     in the cluster.
    * @param rpcCommand   the {@link ReplicableCommand} to send.
    * @param deliverOrder the {@link DeliverOrder} to use.
    * @throws Exception if there was problem sending the request.
    */
   void sendToMany(Collection<Address> destinations, ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception;

   /**
    * @deprecated Use {@link #invokeRemotely(Map, ResponseMode, long, ResponseFilter, DeliverOrder, boolean)} instead
    */
   @Deprecated
   Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode, long timeout,
                                         boolean usePriorityQueue, ResponseFilter responseFilter, boolean totalOrder,
                                         boolean anycast) throws Exception;

   Map<Address, Response> invokeRemotely(Map<Address, ReplicableCommand> rpcCommands, ResponseMode mode,
                                         long timeout, ResponseFilter responseFilter,
                                         DeliverOrder deliverOrder, boolean anycast) throws Exception;


   BackupResponse backupRemotely(Collection<XSiteBackup> backups, XSiteReplicateCommand rpcCommand) throws Exception;

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
    * Retrieves the current cache instance's physical network addresses. Some implementations might differentiate
    * between logical and physical addresses in which case, this method allows clients to query the physical ones
    * associated with the logical address. Implementations where logical and physical address are the same will simply
    * return a single entry List that contains the same Address as {@link #getAddress()}.
    *
    * @return an List of Address
    */
   List<Address> getPhysicalAddresses();

   /**
    * Returns a list of  members in the current cluster view.
    *
    * @return a list of members.  Typically, this would be defensively copied.
    */
   List<Address> getMembers();

   /**
    * Tests whether the transport supports true multicast
    *
    * @return true if the transport supports true multicast
    */
   boolean isMulticastCapable();

   @Override
   @Start(priority = 10)
   void start();

   @Override
   @Stop
   void stop();

   /**
    * @throws org.infinispan.commons.CacheException if the transport has been stopped.
    */
   int getViewId();

   void waitForView(int viewId) throws InterruptedException;

   Log getLog();

   /**
    * check if the transport has configured with total order deliver properties (has the sequencer in JGroups
    * protocol stack.
    */
   void checkTotalOrderSupported();
}
