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
package org.infinispan.remoting;

import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferException;

/**
 * A globally scoped component, that is able to locate named caches and invoke remotely originating calls on the
 * appropriate cache.  The primary goal of this component is to act as a bridge between the globally scoped {@link
 * org.infinispan.remoting.rpc.RpcManager} and named-cache scoped components.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface InboundInvocationHandler {

   /**
    * Invokes a command on the cache, from a remote source.
    *
    * @param command command to invoke
    * @return results, if any, from the invocation
    * @throws Throwable in the event of problems executing the command
    */
   Response handle(CacheRpcCommand command, Address origin) throws Throwable;

   /**
    * Applies state onto a named cache.  State to be read from the stream.  Implementations should NOT close the stream
    * after use.
    *
    * @param cacheName name of cache to apply state
    * @param i         stream to read from
    * @throws StateTransferException in the event of problems
    */
   void applyState(String cacheName, InputStream i) throws StateTransferException;

   /**
    * Generates state from a named cache.  State to be written to the stream.  Implementations should NOT close the
    * stream after use.
    *
    * @param cacheName name of cache from which to generate state
    * @param o         stream to write state to
    * @throws StateTransferException in the event of problems
    */
   void generateState(String cacheName, OutputStream o) throws StateTransferException;

   /**
    * Calling this method should block if the invocation handler implementation has been queueing commands for a given
    * named cache and is in the process of flushing this queue.  It would block until the queue has been drained.
    * @param cacheName name of the cache for which the handler would be queueing requests.
    */
   void blockTillNoLongerRetrying(String cacheName);
}
