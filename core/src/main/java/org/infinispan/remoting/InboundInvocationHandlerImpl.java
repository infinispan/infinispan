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
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.DistributionManager;
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
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ResponseGenerator;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.DistributedSync;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.InputStream;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.factories.KnownComponentNames.*;

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
   private DistributedSync distributedSync;
   private long distributedSyncTimeout;

   // TODO this timeout needs to be configurable.  Should be shorter than your typical lockAcquisitionTimeout/SyncReplTimeout with some consideration for network latency bothfor req and response.
   private static final long timeBeforeWeEnqueueCallForRetry = 10000;

   private final Map<String, RetryQueue> retryThreadMap = Collections.synchronizedMap(new HashMap<String, RetryQueue>());
   private volatile boolean stopping;

   /**
    * How to handle an invocation based on the join status of a given cache *
    */
   private enum JoinHandle {
      QUEUE, OK, IGNORE
   }

   @Inject
   public void inject(GlobalComponentRegistry gcr,
                      @ComponentName(GLOBAL_MARSHALLER) StreamingMarshaller marshaller,
                      EmbeddedCacheManager embeddedCacheManager, Transport transport,
                      GlobalConfiguration globalConfiguration) {
      this.gcr = gcr;
      this.marshaller = marshaller;
      this.embeddedCacheManager = embeddedCacheManager;
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
   }

   @Start
   public void start() {
      distributedSync = transport.getDistributedSync();
      distributedSyncTimeout = globalConfiguration.getDistributedSyncTimeout();
      stopping = false;
   }

   @Stop
   public void stop() {
      stopping = true;
      for (Map.Entry<String, RetryQueue> retryThread : retryThreadMap.entrySet()) {
         retryThread.getValue().interrupt();
      }
   }

   private boolean isDefined(String cacheName) {
      return CacheContainer.DEFAULT_CACHE_NAME.equals(cacheName) || embeddedCacheManager.getCacheNames().contains(cacheName);
   }

   public void waitForStart(CacheRpcCommand cmd) throws InterruptedException {
      if (cmd.getConfiguration().getCacheMode().isDistributed()) {
         cmd.getComponentRegistry().getComponent(DistributionManager.class).waitForJoinToComplete();
      }
   }

   @Override
   public Response handle(final CacheRpcCommand cmd, Address origin) throws Throwable {
   	cmd.setOrigin(origin);
      String cacheName = cmd.getCacheName();
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);

      if (cr == null) {
         if (embeddedCacheManager.getGlobalConfiguration().isStrictPeerToPeer()) {
            // lets see if the cache is *defined* and perhaps just not started.
            if (isDefined(cacheName)) {
               log.waitForCacheToStart(cacheName);
               long giveupTime = System.currentTimeMillis() + 30000; // arbitrary (?) wait time for caches to start
               while (cr == null && System.currentTimeMillis() < giveupTime && !stopping) {
                  Thread.sleep(100);
                  cr = gcr.getNamedComponentRegistry(cacheName);
               }
            }
         }

         if (cr == null) {
            if (log.isInfoEnabled()) log.namedCacheDoesNotExist(cacheName);
            return new ExceptionResponse(new NamedCacheNotFoundException(cacheName, "Cannot process command " + cmd + " on node " + transport.getAddress()));
         }
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
         return new ExceptionResponse(e);
      }
   }

   private Response handleWithWaitForBlocks(CacheRpcCommand cmd, long distSyncTimeout) throws Throwable {
      DistributedSync.SyncResponse sr = distributedSync.blockUntilReleased(distSyncTimeout, MILLISECONDS);

      // If this thread blocked during a NBST flush, then inform the sender
      // it needs to replay ignored messages
      boolean replayIgnored = sr == DistributedSync.SyncResponse.STATE_ACHIEVED;

      Response resp = handleInternal(cmd);

      // A null response is valid and OK ...
      if (resp == null || resp.isValid()) {
         if (replayIgnored) resp = new ExtendedResponse(resp, true);
      } else {
         // invalid response
         if (trace) log.trace("Unable to execute command, got invalid response");
      }

      return resp;
   }

   public JoinHandle howToHandle(CacheRpcCommand cmd) {
      Configuration localConfig = cmd.getConfiguration();
      ComponentRegistry cr = cmd.getComponentRegistry();

      if (localConfig.getCacheMode().isDistributed()) {
         DistributionManager dm = cr.getComponent(DistributionManager.class);
         if (dm.isJoinComplete())
            return JoinHandle.OK;
         else {
            // no point in enqueueing clustered GET commands - just ignore these and hope someone else in the cluster responds.
            if (!(cmd instanceof ClusteredGetCommand))
               return JoinHandle.QUEUE;
            else
               return JoinHandle.IGNORE;
         }
      } else {
         long giveupTime = System.currentTimeMillis() + localConfig.getStateRetrievalTimeout();
         while (cr.getStatus().startingUp() && System.currentTimeMillis() < giveupTime)
            LockSupport.parkNanos(MILLISECONDS.toNanos(100));
         if (!cr.getStatus().allowInvocations()) {
            log.cacheCanNotHandleInvocations(cmd.getCacheName(), cr.getStatus());
            return JoinHandle.IGNORE;
         }

         return JoinHandle.OK;
      }
   }

   @Override
   public void applyState(String cacheName, InputStream i) throws StateTransferException {
      getStateTransferManager(cacheName).applyState(i);
   }

   @Override
   public void generateState(String cacheName, OutputStream o) throws StateTransferException {
      StateTransferManager manager = getStateTransferManager(cacheName);
      if (manager == null) {
         ObjectOutput oo = null;
         try {
            oo = marshaller.startObjectOutput(o, false);
            // Not started yet, so send started flag false
            marshaller.objectToObjectStream(false, oo);
         } catch (Exception e) {
            throw new StateTransferException(e);
         } finally {
            marshaller.finishObjectOutput(oo);
         }
      } else {
         manager.generateState(o);
      }
   }

   private StateTransferManager getStateTransferManager(String cacheName) {
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      if (cr == null)
         return null;
      return cr.getComponent(StateTransferManager.class);
   }

   @Override
   public void blockTillNoLongerRetrying(String cacheName) {
      RetryQueue rq = getRetryQueue(cacheName);
      rq.blockUntilNoLongerRetrying();
   }

   private Response handleWithRetry(final CacheRpcCommand cmd) throws Throwable {
      boolean unlock = false;
      String cacheName = cmd.getCacheName();
      try {
         // We want to retry put operations after the cache has started up
         // We ignore read calls and distributed tasks before the cache has been started
         // as in both cases a a late response won't be useful to the caller.
         // RehashControlCommands are the mechanism used for joining the cluster,
         // so they need to go through immediately (they also ignore the processing lock).
         boolean isRehashCommand = cmd instanceof RehashControlCommand;
         boolean isClusteredGetCommand = cmd instanceof ClusteredGetCommand;
         boolean isDistributedExecuteCommand = cmd instanceof SingleRpcCommand && ((SingleRpcCommand)cmd).getCommand() instanceof DistributedExecuteCommand;
         boolean needRetry = !(isRehashCommand || isClusteredGetCommand || isDistributedExecuteCommand);
         if (!needRetry) {
            try {
               if (!isRehashCommand) {
                  distributedSync.acquireProcessingLock(false, distributedSyncTimeout, MILLISECONDS);
                  unlock = true;
               }
               return handleWithWaitForBlocks(cmd, distributedSyncTimeout);
            } catch (TimeoutException te) {
               log.ignoreClusterGetCall(cmd, Util.prettyPrintTime(distributedSyncTimeout));
               return RequestIgnoredResponse.INSTANCE;
            }
         } else {
            boolean unlockRQLock;
            getRetryQueue(cacheName).retryQueueLock.lock();
            unlockRQLock = true;
            try {
               if (enqueueing(cacheName)) {
                  return enqueueCommand(cmd);
               } else {
                  try {
                     getRetryQueue(cacheName).retryQueueLock.unlock();
                     unlockRQLock = false;
                     switch (howToHandle(cmd)) {
                        case OK:
                           distributedSync.acquireProcessingLock(false, timeBeforeWeEnqueueCallForRetry, MILLISECONDS);
                           unlock = true;
                           return handleWithWaitForBlocks(cmd, distributedSyncTimeout);
                        case QUEUE:
                           return enqueueCommand(cmd);
                        default:
                           return RequestIgnoredResponse.INSTANCE;
                     }

                  } catch (TimeoutException te) {
                     // Enqueue this request rather than wait for this lock...
                     return enqueueCommand(cmd);
                  }
               }
            } finally {
               if (unlockRQLock) getRetryQueue(cacheName).retryQueueLock.unlock();
            }
         }
      } finally {
         if (unlock) distributedSync.releaseProcessingLock(false);
      }
   }

   RetryQueue getRetryQueue(String cacheName) {
      synchronized (retryThreadMap) {
         if (retryThreadMap.containsKey(cacheName))
            return retryThreadMap.get(cacheName);
         else {
            RetryQueue rq = new RetryQueue(cacheName, transport.getAddress().toString());
            retryThreadMap.put(cacheName, rq);
            return rq;
         }
      }
   }

   private boolean enqueueing(String cacheName) {
      return getRetryQueue(cacheName).enqueueing;
   }

   private Response enqueueCommand(CacheRpcCommand command) throws Throwable {
      return getRetryQueue(command.getCacheName()).enqueue(command);
   }

   private class RetryQueue extends Thread {
      boolean enqueueing = false;
      final BlockingQueue<CacheRpcCommand> queue = new LinkedBlockingQueue<CacheRpcCommand>();
      final ReentrantLock retryQueueLock = new ReentrantLock();
      final ReclosableLatch enqueuedBlocker = new ReclosableLatch(true);

      private RetryQueue(String cacheName, String cacheAddress) {
         super("RetryQueueProcessor-" + (cacheName.equals(CacheContainer.DEFAULT_CACHE_NAME) ? "DEFAULT" : cacheName) + "@" + cacheAddress);
         setDaemon(true);
         setPriority(Thread.MAX_PRIORITY);
         super.start();
      }

      public Response enqueue(CacheRpcCommand command) throws Throwable {
         retryQueueLock.lock();
         boolean unlock = false;
         try {
            if (enqueueing) {
               log.tracef("Enqueueing command %s since we are enqueueing.", command);
               queue.add(command);
               return RequestIgnoredResponse.INSTANCE;
            } else {
               try {
                  if (howToHandle(command) == JoinHandle.QUEUE) {
                     enqueueing = true;
                     enqueuedBlocker.close();
                     return enqueue(command);
                  } else {
                     distributedSync.acquireProcessingLock(false, timeBeforeWeEnqueueCallForRetry, MILLISECONDS);
                     unlock = true;
                     return handleWithWaitForBlocks(command, distributedSyncTimeout);
                  }
               } catch (TimeoutException te) {
                  enqueueing = true;
                  enqueuedBlocker.close();
                  return enqueue(command);
               }
            }
         } finally {
            if (unlock) distributedSync.releaseProcessingLock(false);
            retryQueueLock.unlock();
         }
      }

      @Override
      public void run() {
         while (!interrupted()) {
            CacheRpcCommand c = null;
            boolean unlock = false;
            try {
               c = queue.take();
               waitForStart(c);
               distributedSync.acquireProcessingLock(false, distributedSyncTimeout, MILLISECONDS);
               unlock = true;

               handleInternal(c);
               retryQueueLock.lock();
               if (queue.isEmpty()) {
                  enqueueing = false;
                  enqueuedBlocker.open();
               }
               retryQueueLock.unlock();
            } catch (InterruptedException e) {
               enqueueing = false;
               enqueuedBlocker.open();
               // set the interrupted flag
               interrupt();
            } catch (Throwable throwable) {
               log.exceptionHandlingCommand(c, throwable);
            } finally {
               if (unlock) distributedSync.releaseProcessingLock(false);
            }
         }
      }

      public void blockUntilNoLongerRetrying() {
         try {
            enqueuedBlocker.await();
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }
   }
}
