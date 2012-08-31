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
package org.infinispan.remoting.rpc;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.Map;

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
    * @return a map of responses from each member contacted.
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter);

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
    * @return a map of responses from each member contacted.
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue);

   /**
    * Invokes an RPC call on other caches in the cluster.
    *
    * @param recipients a list of Addresses to invoke the call on.  If this is null, the call is broadcast to the entire
    *                   cluster.
    * @param rpcCommand the cache command to invoke
    * @param mode       the response mode to use
    * @param timeout    a timeout after which to throw a replication exception.
    * @return a map of responses from each member contacted.
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout);

   /**
    * Broadcasts an RPC command to the entire cluster.
    *
    * @param rpc  command to execute remotely
    * @param sync if true, the transport will operate in sync mode.  Otherwise, it will operate in async mode.
    * @throws org.infinispan.remoting.RpcException in the event of problems
    */
   void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws RpcException;

   /**
    * Broadcasts an RPC command to the entire cluster.
    *
    * @param rpc              command to execute remotely
    * @param sync             if true, the transport will operate in sync mode.  Otherwise, it will operate in async
    *                         mode.
    * @param usePriorityQueue if true, a priority queue is used
    * @throws org.infinispan.remoting.RpcException in the event of problems
    */
   void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException;

   /**
    * The same as {@link #broadcastRpcCommand(org.infinispan.commands.ReplicableCommand, boolean)} except that the task
    * is passed to the transport executor and a Future is returned.  The transport always deals with this
    * synchronously.
    *
    * @param rpc    command to execute remotely
    * @param future the future which will be passed back to the user
    */
   void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future);

   /**
    * The same as {@link #broadcastRpcCommand(org.infinispan.commands.ReplicableCommand, boolean, boolean)} except that
    * the task is passed to the transport executor and a Future is returned.  The transport always deals with this
    * synchronously.
    *
    * @param rpc              command to execute remotely
    * @param usePriorityQueue if true, a priority queue is used
    * @param future           the future which will be passed back to the user
    */
   void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future);

   /**
    * Broadcasts an RPC command to a specified set of recipients
    *
    * @param recipients recipients to invoke remote command on
    * @param rpc        command to execute remotely
    * @param sync       if true, the transport will operate in sync mode.  Otherwise, it will operate in async mode.
    * @throws org.infinispan.remoting.RpcException in the event of problems
    */
   void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws RpcException;

   /**
    * Broadcasts an RPC command to a specified set of recipients
    *
    * @param recipients       recipients to invoke remote command on
    * @param rpc              command to execute remotely
    * @param sync             if true, the transport will operate in sync mode.  Otherwise, it will operate in async
    *                         mode.
    * @param usePriorityQueue if true, a priority queue is used
    * @throws org.infinispan.remoting.RpcException in the event of problems
    */
   Map<Address, Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws RpcException;

   /**
    * The same as {@link #invokeRemotely(java.util.Collection, org.infinispan.commands.ReplicableCommand, boolean)}
    * except that the task is passed to the transport executor and a Future is returned.  The transport always deals
    * with this synchronously.
    *
    * @param recipients recipients to invoke remote call on
    * @param rpc        command to execute remotely
    * @param future     the future which will be passed back to the user
    */
   void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future);

   /**
    * The same as {@link #invokeRemotely(java.util.Collection, org.infinispan.commands.ReplicableCommand, boolean)}
    * except that the task is passed to the transport executor and a Future is returned.  The transport always deals
    * with this synchronously.
    *
    * @param recipients       recipients to invoke remote call on
    * @param rpc              command to execute remotely
    * @param usePriorityQueue if true, a priority queue is used
    * @param future           the future which will be passed back to the user
    */
   void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future);

   /**
    * The same as {@link #invokeRemotelyInFuture(java.util.Collection, org.infinispan.commands.ReplicableCommand,
    * boolean, org.infinispan.util.concurrent.NotifyingNotifiableFuture)} except that you can specify a timeout.
    *
    * @param recipients       recipients to invoke remote call on
    * @param rpc              command to execute remotely
    * @param usePriorityQueue if true, a priority queue is used
    * @param future           the future which will be passed back to the user
    * @param timeout          after which to give up (in millis)
    */
   void invokeRemotelyInFuture(final Collection<Address> recipients, final ReplicableCommand rpc, final boolean usePriorityQueue, final NotifyingNotifiableFuture<Object> future, final long timeout);

   /**
    * The same as {@link #invokeRemotelyInFuture(java.util.Collection, org.infinispan.commands.ReplicableCommand,
    * boolean, org.infinispan.util.concurrent.NotifyingNotifiableFuture, long)} except that you can specify a response mode.
    *
    * @param recipients       recipients to invoke remote call on
    * @param rpc              command to execute remotely
    * @param usePriorityQueue if true, a priority queue is used
    * @param future           the future which will be passed back to the user
    * @param timeout          after which to give up (in millis)
    * @param ignoreLeavers    if {@code true}, recipients that leave or have already left the cluster are ignored
    *                         if {@code false}, a {@code SuspectException} is thrown when a leave is detected
    */
   void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc,
                               boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future,
                               long timeout, boolean ignoreLeavers);

   /**
    * @return a reference to the underlying transport.
    */
   Transport getTransport();

   /**
    * Returns the address associated with this RpcManager or null if not part of the cluster.
    */
   Address getAddress();
}