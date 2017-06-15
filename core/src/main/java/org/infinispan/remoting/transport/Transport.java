package org.infinispan.remoting.transport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.Experimental;
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

   /**
    * @return A {@link CompletableFuture} that completes when the transport has installed the expected view.
    */
   CompletableFuture<Void> withView(int expectedViewId);

   /**
    * @deprecated Since 9.0, please use {@link #withView(int)} instead.
    */
   @Deprecated
   void waitForView(int viewId) throws InterruptedException;

   Log getLog();

   /**
    * check if the transport has configured with total order deliver properties (has the sequencer in JGroups
    * protocol stack.
    */
   void checkTotalOrderSupported();

   /**
    * Get the view of interconnected sites.
    * If no cross site replication has been configured, this method returns null.
    *
    * Inspecting the site view can be useful to see if the different sites
    * have managed to join each other, which is pre-requisite to get cross
    * replication working.
    *
    * @return set containing the connected sites, or null if no cross site
    *         replication has been enabled.
    */
   Set<String> getSitesView();

   /**
    * Invoke a command on a single node and pass the response to a {@link ResponseCollector}.
    *
    * If the target is the local node and the delivery order is not {@link DeliverOrder#TOTAL},
    * the command is never executed, and {@link ResponseCollector#finish()} is called directly.
    *
    * @since 9.1
    */
   @Experimental
   <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                        ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                        long timeout, TimeUnit unit);

   /**
    * Invoke a command on a collection of node and pass the responses to a {@link ResponseCollector}.
    *
    * If one of the targets is the local nodes and the delivery order is not {@link DeliverOrder#TOTAL},
    * the command is only executed on the remote nodes.
    *
    * @since 9.1
    */
   @Experimental
   <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                        ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                        long timeout, TimeUnit unit);

   /**
    * Invoke a command on all the nodes in the cluster and pass the responses to a {@link ResponseCollector}.
    *
    * The command is only executed on the local node if the delivery order is {@link DeliverOrder#TOTAL}.
    *
    * @since 9.1
    */
   @Experimental
   <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<T> collector,
                                             DeliverOrder deliverOrder, long timeout, TimeUnit unit);

   /**
    * Invoke a command on a collection of nodes and pass the responses to a {@link ResponseCollector}.
    *
    * The command is only sent immediately to the first target, and there is an implementation-dependent
    * delay before sending the command to each target. There is no delay if the target responds or leaves
    * the cluster. The remaining targets are skipped if {@link ResponseCollector#addResponse(Address, Response)}
    * returns a non-{@code null} value.
    *
    * If one of the targets is the local node and the delivery order is not {@link DeliverOrder#TOTAL},
    * the command is only executed on the remote nodes.
    *
    * @since 9.1
    */
   @Experimental
   <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                 ResponseCollector<T> collector, DeliverOrder deliverOrder,
                                                 long timeout, TimeUnit unit);

   /**
    * Invoke different commands on a collection of nodes and pass the responses to a {@link ResponseCollector}.
    *
    * If one of the targets is the local node and the delivery order is not {@link DeliverOrder#TOTAL},
    * the command is only executed on the remote nodes.
    *
    * @since 9.1
    */
   @Experimental
   <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                         Function<Address, ReplicableCommand> commandGenerator,
                                         ResponseCollector<T> responseCollector, long timeout,
                                         DeliverOrder deliverOrder);
}
