/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.remoting.transport;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.util.logging.Log;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.*;

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
   // TODO discovery should be abstracted away into a separate set of interfaces such that it is not tightly coupled to the transport

   @Inject
   void setConfiguration(GlobalConfiguration gc);

   /**
    * Initializes the transport with global cache configuration and transport-specific properties.
    *
    * @param marshaller    marshaller to use for marshalling and unmarshalling
    * @param asyncExecutor executor to use for asynchronous calls
    * @param handler       handler for invoking remotely originating calls on the local cache
    * @param notifier      notifier to use
    */
   @Inject
   void initialize(@ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller marshaller,
                   @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncExecutor,
                   InboundInvocationHandler handler, CacheManagerNotifier notifier);

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
    * @return a map of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout,
                                 boolean usePriorityQueue, ResponseFilter responseFilter, boolean supportReplay) throws Exception;

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
    * Initiates a state retrieval from a specific cache (by typically invoking {@link
    * org.infinispan.remoting.InboundInvocationHandler#generateState(String, java.io.OutputStream)}), and applies this
    * state to the current cache via the  {@link InboundInvocationHandler#applyState(String, java.io.InputStream)}
    * callback.
    *
    * @param cacheName name of cache for which to retrieve state
    * @param address   address of remote cache from which to retrieve state
    * @param timeout   state retrieval timeout in milliseconds
    * @return true if state was transferred and applied successfully, false if it timed out.
    * @throws org.infinispan.statetransfer.StateTransferException
    *          if state cannot be retrieved from the specific cache
    */
   boolean retrieveState(String cacheName, Address address, long timeout) throws StateTransferException;

   /**
    * @return an instance of a DistributedSync that can be used to wait for synchronization events across a cluster.
    */
   DistributedSync getDistributedSync();

   /**
    * Tests whether the transport supports state transfer
    *
    * @return true if the implementation supports state transfer, false otherwise.
    */
   boolean isSupportStateTransfer();
   
   /**
    * Tests whether the transport supports true multicast
    * 
    * @return true if the transport supports true multicast
    */
   boolean isMulticastCapable();

   @Start(priority = 10)
   void start();

   @Stop
   void stop();

   /**
    * TODO: Document
    *
    * @return
    */
   int getViewId();

   /**
    * TODO: Document
    *
    * @return
    */
   Log getLog();

}
