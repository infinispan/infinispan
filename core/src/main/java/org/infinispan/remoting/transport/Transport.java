package org.infinispan.remoting.transport;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.raft.RaftManager;
import org.infinispan.util.logging.Log;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.commands.remote.XSiteRequest;

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

   CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                 ReplicableCommand rpcCommand,
                                                                 ResponseMode mode, long timeout,
                                                                 ResponseFilter responseFilter,
                                                                 DeliverOrder deliverOrder) throws Exception;

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
    * Asynchronously sends the {@link ReplicableCommand} to the entire cluster.
    *
    * @since 9.2
    */
   @Experimental
   default void sendToAll(ReplicableCommand rpcCommand, DeliverOrder deliverOrder) throws Exception {
      sendToMany(null, rpcCommand, deliverOrder);
   }

   /**
    * Sends a cross-site request to a remote site.
    * <p>
    * Currently, no reply values are supported. Or the request completes successfully or it throws an {@link
    * Exception}.
    * <p>
    * If {@link XSiteBackup#isSync()} returns {@code false}, the {@link XSiteResponse} is only completed when the an
    * ACK from the remote site is received. The invoker needs to make sure not to wait for the {@link XSiteResponse}.
    *
    * @param backup     The remote site.
    * @param rpcCommand The command to send.
    * @return A {@link XSiteResponse} that is completed when the request is completed.
    */
   <O> XSiteResponse<O> backupRemotely(XSiteBackup backup, XSiteRequest<O> rpcCommand);

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
   List<PhysicalAddress> getPhysicalAddresses();

   /**
    * Returns a list of  members in the current cluster view.
    *
    * @return a list of members.  Typically, this would be defensively copied.
    */
   List<Address> getMembers();


   /**
    * Returns physical addresses of members in the current cluster view.
    *
    * @return a list of physical addresses
    */
   List<PhysicalAddress> getMembersPhysicalAddresses();

   /**
    * Tests whether the transport supports true multicast
    *
    * @return true if the transport supports true multicast
    */
   boolean isMulticastCapable();

   /**
    * Checks if this {@link Transport} is able to perform cross-site requests.
    *
    * @throws CacheConfigurationException if cross-site isn't available.
    */
   void checkCrossSiteAvailable() throws CacheConfigurationException;

   /**
    * @return The local site name or {@code null} if this {@link Transport} cannot make cross-site requests.
    */
   String localSiteName();

   /**
    * @return The local node name, defaults to the local node address.
    */
   default String localNodeName() {
      return getAddress().toString();
   }

   @Override
   void start();

   @Override
   void stop();

   /**
    * @throws org.infinispan.commons.CacheException if the transport has been stopped.
    */
   int getViewId();

   /**
    * @return A {@link CompletableFuture} that completes when the transport has installed the expected view.
    */
   CompletableFuture<Void> withView(int expectedViewId);

   Log getLog();

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
    * @return {@code true} if this node is a cross-site replication coordinator.
    */
   boolean isSiteCoordinator();

   /**
    * @return The current site coordinators {@link Address}.
    */
   Collection<Address> getRelayNodesAddress();

   default boolean isPrimaryRelayNode() {
      var relayNodeList = getRelayNodesAddress();
      return !relayNodeList.isEmpty() && relayNodeList.iterator().next().equals(getAddress());
   }

   /**
    * Invoke a command on a single node and pass the response to a {@link ResponseCollector}.
    * <p>
    * If the target is the local node, the command is never executed and {@link ResponseCollector#finish()} is called directly.
    *
    * @since 9.1
    */
   @Experimental
   default <T> CompletionStage<T> invokeCommand(Address target, ReplicableCommand command,
                                                ResponseCollector<Address, T> collector, DeliverOrder deliverOrder,
                                                long timeout, TimeUnit unit) {
      // Implement the new methods on top of invokeRemotelyAsync to support custom implementations
      return invokeCommand(Collections.singleton(target), command, collector, deliverOrder, timeout, unit);
   }

   /**
    * Invoke a command on a collection of node and pass the responses to a {@link ResponseCollector}.
    * <p>
    * If one of the targets is the local node, it is ignored. The command is only executed on the remote nodes.
    *
    * @since 9.1
    */
   @Experimental
   default <T> CompletionStage<T> invokeCommand(Collection<Address> targets, ReplicableCommand command,
                                                ResponseCollector<Address, T> collector, DeliverOrder deliverOrder,
                                                long timeout, TimeUnit unit) {
      // Implement the new methods on top of invokeRemotelyAsync to support custom implementations
      try {
         return invokeRemotelyAsync(targets, command, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
                                    unit.toMillis(timeout), null, deliverOrder)
                   .thenApply(map -> {
                      for (Map.Entry<Address, Response> e : map.entrySet()) {
                         T result = collector.addResponse(e.getKey(), e.getValue());
                         if (result != null)
                            return result;
                      }
                      return collector.finish();
                   });
      } catch (Exception e) {
         throw Util.rewrapAsCacheException(e);
      }
   }

   /**
    * Invoke a command on all the nodes in the cluster and pass the responses to a {@link ResponseCollector}.
    * <p>
    * The command is not executed locally and it is not sent across RELAY2 bridges to remote sites.
    *
    * @since 9.1
    */
   @Experimental
   default <T> CompletionStage<T> invokeCommandOnAll(ReplicableCommand command, ResponseCollector<Address, T> collector,
                                                     DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      return invokeCommand(getMembers(), command, collector, deliverOrder, timeout, unit);
   }

   /**
    * Invoke a command on all the nodes in the cluster and pass the responses to a {@link ResponseCollector}.
    * <p>
    * he command is not executed locally and it is not sent across RELAY2 bridges to remote sites.
    *
    * @since 9.3
    */
   @Experimental
   default <T> CompletionStage<T> invokeCommandOnAll(Collection<Address> requiredTargets, ReplicableCommand command,
                                                       ResponseCollector<Address, T> collector, DeliverOrder deliverOrder,
                                                       long timeout, TimeUnit unit) {
      return invokeCommand(requiredTargets, command, collector, deliverOrder, timeout, unit);
   }

   /**
    * Invoke a command on a collection of nodes and pass the responses to a {@link ResponseCollector}.
    * <p>
    * The command is only sent immediately to the first target, and there is an implementation-dependent
    * delay before sending the command to each target. There is no delay if the target responds or leaves
    * the cluster. The remaining targets are skipped if {@link ResponseCollector#addResponse(Object, Response)}
    * returns a non-{@code null} value.
    * <p>
    * The command is only executed on the remote nodes.
    *
    * @since 9.1
    */
   @Experimental
   default <T> CompletionStage<T> invokeCommandStaggered(Collection<Address> targets, ReplicableCommand command,
                                                         ResponseCollector<Address, T> collector, DeliverOrder deliverOrder,
                                                         long timeout, TimeUnit unit) {
      // Implement the new methods on top of invokeRemotelyAsync to support custom implementations
      AtomicReference<Object> result = new AtomicReference<>(null);
      try {
         ResponseFilter responseFilter = new ResponseFilter() {
            @Override
            public boolean isAcceptable(Response response, Address sender) {
               // Guarantee collector.addResponse() isn't called concurrently
               synchronized (result) {
                  if (result.get() != null)
                     return false;

                  T t = collector.addResponse(sender, response);
                  result.set(t);
                  return t != null;
               }
            }

            @Override
            public boolean needMoreResponses() {
               return result.get() == null;
            }
         };
         return invokeRemotelyAsync(targets, command, ResponseMode.WAIT_FOR_VALID_RESPONSE,
                                    unit.toMillis(timeout), responseFilter, deliverOrder)
                   .thenApply(map -> {
                      synchronized (result) {
                         if (result.get() != null) {
                            return (T) result.get();
                         } else {
                            // Prevent further calls to collector.addResponse()
                            result.set(new Object());
                            return collector.finish();
                         }
                      }
                   });
      } catch (Exception e) {
         throw Util.rewrapAsCacheException(e);
      }
   }

   /**
    * Invoke different commands on a collection of nodes and pass the responses to a {@link ResponseCollector}.
    * <p>
    * The command is only executed on the remote nodes.
    *
    * @since 9.2
    */
   @Experimental
   default <T> CompletionStage<T> invokeCommands(Collection<Address> targets,
                                                 Function<Address, ReplicableCommand> commandGenerator,
                                                 ResponseCollector<Address, T> collector, DeliverOrder deliverOrder,
                                                 long timeout, TimeUnit timeUnit) {
      AtomicReference<Object> result = new AtomicReference<>(null);
      ResponseCollector<Address, T> partCollector = new ResponseCollector<>() {
         @Override
         public T addResponse(Address sender, Response response) {
            synchronized (this) {
               if (result.get() != null)
                  return null;

               result.set(collector.addResponse(sender, response));
               return null;
            }
         }

         @Override
         public T finish() {
            // Do nothing when individual commands finish
            return null;
         }
      };

      AggregateCompletionStage<Void> allStage = CompletionStages.aggregateCompletionStage();
      for (Address target : targets) {
         allStage.dependsOn(invokeCommand(target, commandGenerator.apply(target), partCollector, deliverOrder,
                                          timeout, timeUnit));
      }
      return allStage.freeze().thenApply(v -> {
         synchronized (partCollector) {
            if (result.get() != null) {
               return (T) result.get();
            } else {
               return collector.finish();
            }
         }
      });
   }

   /**
    * @return The {@link RaftManager} instance,
    */
   RaftManager raftManager();

   /**
    * @deprecated Use {@link #invokeRemotelyAsync(Collection, ReplicableCommand, ResponseMode, long, ResponseFilter,
    * DeliverOrder)}.
    */
   @Deprecated(forRemoval=true, since = "16.0")
   default CompletableFuture<Map<Address, Response>> invokeRemotelyAsync(Collection<Address> recipients,
                                                                 ReplicableCommand rpcCommand,
                                                                 ResponseMode mode, long timeout,
                                                                 ResponseFilter responseFilter,
                                                                 DeliverOrder deliverOrder,
                                                                 boolean anycast) throws Exception {
      return invokeRemotelyAsync(recipients, rpcCommand, mode, timeout, responseFilter, deliverOrder);
   }
}
