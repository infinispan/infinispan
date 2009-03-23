/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.remoting;

import org.horizon.commands.ReplicableCommand;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.lifecycle.Lifecycle;
import org.horizon.remoting.transport.Address;
import org.horizon.remoting.transport.Transport;
import org.horizon.statetransfer.StateTransferException;

import java.util.List;

/**
 * Provides a mechanism for communicating with other caches in the cluster.
 * <p/>
 * Implementations have a simple lifecycle: <ul> <li>start() - starts the underlying communication channel based on
 * configuration options injected, and connects the channel</li> <li>stop() - stops the dispatcher and releases
 * resources</li> </ul>
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@NonVolatile
public interface RPCManager extends Lifecycle {
   // TODO this needs to be re-thought regarding adding a transport-independent mechanism of unicasts for distribution based on consistent hashes
   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients           a list of Addresses to invoke the call on.  If this is null, the call is broadcast to
    *                             the entire cluster.
    * @param rpcCommand           the cache command to invoke
    * @param mode                 the response mode to use
    * @param timeout              a timeout after which to throw a replication exception.
    * @param usePriorityQueue     if true, a priority queue is used to deliver messages.  May not be supported by all
    *                             implementations.
    * @param responseFilter       a response filter with which to filter out failed/unwanted/invalid responses.
    * @param stateTransferEnabled if true, additional replaying is considered if messages need to be re-sent during the
    *                             course of a state transfer
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Object> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter, boolean stateTransferEnabled) throws Exception;

   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients           a list of Addresses to invoke the call on.  If this is null, the call is broadcast to
    *                             the entire cluster.
    * @param rpcCommand           the cache command to invoke
    * @param mode                 the response mode to use
    * @param timeout              a timeout after which to throw a replication exception.
    * @param usePriorityQueue     if true, a priority queue is used to deliver messages.  May not be supported by all
    *                             implementations.
    * @param stateTransferEnabled if true, additional replaying is considered if messages need to be re-sent during the
    *                             course of a state transfer
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Object> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, boolean stateTransferEnabled) throws Exception;

   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients           a list of Addresses to invoke the call on.  If this is null, the call is broadcast to
    *                             the entire cluster.
    * @param rpcCommand           the cache command to invoke
    * @param mode                 the response mode to use
    * @param timeout              a timeout after which to throw a replication exception.
    * @param stateTransferEnabled if true, additional replaying is considered if messages need to be re-sent during the
    *                             course of a state transfer
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Object> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean stateTransferEnabled) throws Exception;

   /**
    * Initiates a state retrieval process from neighbouring caches.  This method will block until it either times out,
    * or state is retrieved and applied.
    *
    * @param cacheName name of cache requesting state
    * @param timeout   length of time to try to retrieve state on each peer
    * @throws org.horizon.statetransfer.StateTransferException
    *          in the event of problems
    */
   void retrieveState(String cacheName, long timeout) throws StateTransferException;

   /**
    * @return a reference to the underlying transport.
    */
   Transport getTransport();

   /**
    * If {@link #retrieveState(String, long)} has been invoked and hasn't yet returned (i.e., a state transfer is in
    * progress), this method will return the current Address from which a state transfer is being attempted.  Otherwise,
    * this method returns a null.
    *
    * @return the current Address from which a state transfer is being attempted, if a state transfer is in progress, or
    *         a null otherwise.
    */
   Address getCurrentStateTransferSource();
}