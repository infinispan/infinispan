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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   private GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private GlobalConfiguration globalConfiguration;
   private Transport transport;

   @Inject
   public void inject(GlobalComponentRegistry gcr, Transport transport,
                      GlobalConfiguration globalConfiguration) {
      this.gcr = gcr;
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
   }

   @Override
   public Response handle(final CacheRpcCommand cmd, Address origin) throws Throwable {
      cmd.setOrigin(origin);

      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (!globalConfiguration.transport().strictPeerToPeer()) {
            if (trace) log.tracef("Strict peer to peer off, so silently ignoring that %s cache is not defined", cacheName);
            return null;
         }

         log.namedCacheDoesNotExist(cacheName);
         return new ExceptionResponse(new NamedCacheNotFoundException(cacheName, "Cache has not been started on node " + transport.getAddress()));
      }

      return handleWithRetry(cmd, cr);
   }


   private Response handleInternal(final CacheRpcCommand cmd, final ComponentRegistry cr) throws Throwable {
      CommandsFactory commandsFactory = cr.getCommandsFactory();

      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd, true);

      try {
         if (trace) log.tracef("Calling perform() on %s", cmd);
         ResponseGenerator respGen = cr.getResponseGenerator();
         Object retval = cmd.perform(null);
         return respGen.getResponse(cmd, retval);
      } catch (Exception e) {
         log.trace("Exception executing command", e);
         return new ExceptionResponse(e);
      }
   }

   private Response handleWithWaitForBlocks(final CacheRpcCommand cmd, final ComponentRegistry cr) throws Throwable {
      Response resp = handleInternal(cmd, cr);

      // A null response is valid and OK ...
      if (trace && resp != null && !resp.isValid()) {
         // invalid response
         log.tracef("Unable to execute command, got invalid response %s", resp);
      }

      return resp;
   }

   private Response handleWithRetry(final CacheRpcCommand cmd, final ComponentRegistry componentRegistry) throws Throwable {
      return handleWithWaitForBlocks(cmd, componentRegistry);
   }
}

