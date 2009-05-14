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
package org.infinispan.remoting.rpc;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.ReplicationException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Provides a mechanism for communicating with other caches in the cluster, by formatting and passing requests down to
 * the registered {@link Transport}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface RpcManager {
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
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) throws Exception;

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
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) throws Exception;

   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients a list of Addresses to invoke the call on.  If this is null, the call is broadcast to the entire
    *                   cluster.
    * @param rpcCommand the cache command to invoke
    * @param mode       the response mode to use
    * @param timeout    a timeout after which to throw a replication exception.
    * @return a list of responses from each member contacted.
    * @throws Exception in the event of problems.
    */
   List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) throws Exception;

   /**
    * Initiates a state retrieval process from neighbouring caches.  This method will block until it either times out,
    * or state is retrieved and applied.
    *
    * @param cacheName name of cache requesting state
    * @param timeout   length of time to try to retrieve state on each peer
    * @throws org.infinispan.statetransfer.StateTransferException
    *          in the event of problems
    */
   void retrieveState(String cacheName, long timeout) throws StateTransferException;

   /**
    * Broadcasts an RPC command to the entire cluster.
    *
    * @param rpc  command to execute remotely
    * @param sync if true, the transport will operate in sync mode.  Otherwise, it will operate in async mode.
    * @throws ReplicationException in the event of problems
    */
   void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws ReplicationException;

   /**
    * Broadcasts an RPC command to the entire cluster.
    *
    * @param rpc              command to execute remotely
    * @param sync             if true, the transport will operate in sync mode.  Otherwise, it will operate in async
    *                         mode.
    * @param usePriorityQueue if true, a priority queue is used
    * @throws ReplicationException in the event of problems
    */
   void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException;

   /**
    * The same as {@link #broadcastRpcCommand(org.infinispan.commands.ReplicableCommand, boolean)} except that the task
    * is passed to the transport executor and a Future is returned.  The transport always deals with this
    * synchronously.
    *
    * @param rpc command to execute remotely
    * @return a future
    */
   Future<Object> broadcastRpcCommandInFuture(ReplicableCommand rpc);

   /**
    * The same as {@link #broadcastRpcCommand(org.infinispan.commands.ReplicableCommand, boolean, boolean)} except that
    * the task is passed to the transport executor and a Future is returned.  The transport always deals with this
    * synchronously.
    *
    * @param rpc              command to execute remotely
    * @param usePriorityQueue if true, a priority queue is used
    * @return a future
    */
   Future<Object> broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue);

   /**
    * Broadcasts an RPC command to a specified set of recipients
    *
    * @param recipients recipients to invoke remote command on
    * @param rpc        command to execute remotely
    * @param sync       if true, the transport will operate in sync mode.  Otherwise, it will operate in async mode.
    * @throws ReplicationException in the event of problems
    */
   void anycastRpcCommand(List<Address> recipients, ReplicableCommand rpc, boolean sync) throws ReplicationException;

   /**
    * Broadcasts an RPC command to a specified set of recipients
    *
    * @param recipients       recipients to invoke remote command on
    * @param rpc              command to execute remotely
    * @param sync             if true, the transport will operate in sync mode.  Otherwise, it will operate in async
    *                         mode.
    * @param usePriorityQueue if true, a priority queue is used
    * @throws ReplicationException in the event of problems
    */
   void anycastRpcCommand(List<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException;

   /**
    * The same as {@link #anycastRpcCommand(java.util.List, org.infinispan.commands.ReplicableCommand, boolean)} except
    * that the task is passed to the transport executor and a Future is returned.  The transport always deals with this
    * synchronously.
    *
    * @param recipients recipients to invoke remote call on
    * @param rpc        command to execute remotely
    * @return a future
    */
   Future<Object> anycastRpcCommandInFuture(List<Address> recipients, ReplicableCommand rpc);

   /**
    * The same as {@link #anycastRpcCommand(java.util.List, org.infinispan.commands.ReplicableCommand, boolean)} except
    * that the task is passed to the transport executor and a Future is returned.  The transport always deals with this
    * synchronously.
    *
    * @param recipients       recipients to invoke remote call on
    * @param rpc              command to execute remotely
    * @param usePriorityQueue if true, a priority queue is used
    * @return a future
    */
   Future<Object> anycastRpcCommandInFuture(List<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue);

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