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

import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.CacheViewControlCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.factories.KnownComponentNames.GLOBAL_MARSHALLER;

/**
 * Sets the cache interceptor chain on an RPCCommand before calling it to perform
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class InboundInvocationHandlerImpl implements InboundInvocationHandler {
   GlobalComponentRegistry gcr;
   private static final Log log = LogFactory.getLog(InboundInvocationHandlerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private StreamingMarshaller marshaller;
   private EmbeddedCacheManager embeddedCacheManager;
   private GlobalConfiguration globalConfiguration;
   private Transport transport;
   private CacheViewsManager cacheViewsManager;

   // TODO this timeout needs to be configurable.  Should be shorter than your typical lockAcquisitionTimeout/SyncReplTimeout with some consideration for network latency bothfor req and response.
   private static final long timeBeforeWeEnqueueCallForRetry = 10000;

   private volatile boolean stopping;

   /**
    * How to handle an invocation based on the join status of a given cache *
    */
   private enum JoinHandle {
      OK, IGNORE
   }

   @Inject
   public void inject(GlobalComponentRegistry gcr,
                      @ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller marshaller,
                      EmbeddedCacheManager embeddedCacheManager, Transport transport,
                      GlobalConfiguration globalConfiguration, CacheViewsManager cacheViewsManager) {
      this.gcr = gcr;
      this.marshaller = marshaller;
      this.embeddedCacheManager = embeddedCacheManager;
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
      this.cacheViewsManager = cacheViewsManager;
   }

   @Start
   public void start() {
      stopping = false;
   }

   @Stop
   public void stop() {
      stopping = true;
   }

   private boolean isDefined(String cacheName) {
      return CacheContainer.DEFAULT_CACHE_NAME.equals(cacheName) || embeddedCacheManager.getCacheNames().contains(cacheName);
   }

   public void waitForStart(ComponentRegistry componentRegistry) throws InterruptedException {
      StateTransferManager stateTransferManager = componentRegistry.getComponent(StateTransferManager.class);
      if (stateTransferManager != null) {
         stateTransferManager.waitForJoinToComplete();
      }
   }

   public boolean hasJoinStarted(ComponentRegistry componentRegistry) throws InterruptedException {
      StateTransferManager stateTransferManager = componentRegistry.getComponent(StateTransferManager.class);
      if (stateTransferManager != null) {
         return stateTransferManager.hasJoinStarted();
      } else {
         return true;
      }
   }

   @Override
   public Response handle(final CacheRpcCommand cmd, Address origin) throws Throwable {
      cmd.setOrigin(origin);

      // TODO Support global commands separately
      if (cmd instanceof CacheViewControlCommand) {
         ((CacheViewControlCommand) cmd).init(cacheViewsManager);
         try {
            return new SuccessfulResponse(cmd.perform(null));
         } catch (Exception e) {
            return new ExceptionResponse(e);
         }
      }

      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (!globalConfiguration.isStrictPeerToPeer())
            return null;

         log.namedCacheDoesNotExist(cacheName);
         return new ExceptionResponse(new NamedCacheNotFoundException(cacheName, "Cache has not been started on node " + transport.getAddress()));
      }

      final Configuration localConfig = cr.getComponent(Configuration.class);
      cmd.injectComponents(localConfig, cr);

      return handleWithRetry(cmd);
   }


   private Response handleInternal(CacheRpcCommand cmd) throws Throwable {
      ComponentRegistry cr = cmd.getComponentRegistry();
      CommandsFactory commandsFactory = cr.getLocalComponent(CommandsFactory.class);

      // initialize this command with components specific to the intended cache instance
      commandsFactory.initializeReplicableCommand(cmd, true);

      try {
         log.tracef("Calling perform() on %s", cmd);
         ResponseGenerator respGen = cr.getComponent(ResponseGenerator.class);
         Object retval = cmd.perform(null);
         return respGen.getResponse(cmd, retval);
      } catch (Exception e) {
         log.trace("Exception executing command", e);
         return new ExceptionResponse(e);
      }
   }

   private Response handleWithWaitForBlocks(CacheRpcCommand cmd) throws Throwable {
      Response resp = handleInternal(cmd);

      // A null response is valid and OK ...
      if (trace && resp != null && !resp.isValid()) {
         // invalid response
         log.tracef("Unable to execute command, got invalid response %s", resp);
      }

      return resp;
   }

   private Response handleWithRetry(final CacheRpcCommand cmd) throws Throwable {
      // RehashControlCommands are the mechanism used for joining the cluster,
      // so they don't need to wait until the cache starts up.
      boolean isStateTransferCommand = cmd instanceof StateTransferControlCommand;
      if (!isStateTransferCommand) {
         // For normal commands, reject them if we didn't start joining yet
         if (!hasJoinStarted(cmd.getComponentRegistry())) {
            log.cacheCanNotHandleInvocations(cmd.getCacheName());
            return new ExceptionResponse(new NamedCacheNotFoundException(cmd.getCacheName(),
                  "Cache has not been started on node " + transport.getAddress()));
         }
         // if we did start joining, the StateTransferLockInterceptor will make it wait until the state transfer is complete
         // TODO There is a small window between starting the join and blocking the transactions, we need to eliminate it
         //waitForStart(cmd.getComponentRegistry());
      }
      return handleWithWaitForBlocks(cmd);
   }
}

