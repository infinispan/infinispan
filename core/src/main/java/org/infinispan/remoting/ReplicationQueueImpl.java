/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.configuration.cache.AsyncConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A default implementation of the ReplicationQueue interface.
 *
 * @author Manik Surtani
 * @version 4.2
 */
public class ReplicationQueueImpl implements ReplicationQueue {
   private static final Log log = LogFactory.getLog(ReplicationQueue.class);

   /**
    * Max elements before we flush
    */
   private long maxElements = 500;

   /**
    * Holds the replication jobs.
    */
   private final BlockingQueue<ReplicableCommand> elements = new LinkedBlockingQueue<ReplicableCommand>();

   /**
    * For periodical replication
    */
   private ScheduledExecutorService scheduledExecutor = null;
   private RpcManager rpcManager;
   private Configuration configuration;
   private boolean enabled;
   private CommandsFactory commandsFactory;
   private volatile ScheduledFuture<?> scheduledFuture;
   private boolean trace;
   private String cacheName;

   /**
    * @return true if this replication queue is enabled, false otherwise.
    */
   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Inject
   public void injectDependencies(@ComponentName(KnownComponentNames.ASYNC_REPLICATION_QUEUE_EXECUTOR) ScheduledExecutorService executor,
              RpcManager rpcManager, Configuration configuration, CommandsFactory commandsFactory, Cache cache) {
      injectDependencies(executor, rpcManager, configuration, commandsFactory, cache.getName());
   }

   public void injectDependencies(ScheduledExecutorService executor,
         RpcManager rpcManager, Configuration configuration,
         CommandsFactory commandsFactory, String cacheName) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.commandsFactory = commandsFactory;
      this.scheduledExecutor = executor;
      this.cacheName = cacheName;
   }

   /**
    * Starts the asynchronous flush queue.
    */
   @Override
   @Start
   public void start() {
      AsyncConfiguration asyncCfg = configuration.clustering().async();
      long interval = asyncCfg.replQueueInterval();
      trace = log.isTraceEnabled();
      if (trace)
         log.tracef("Starting replication queue, with interval %d and maxElements %s", interval, maxElements);

      this.maxElements = asyncCfg.replQueueMaxElements();
      // check again
      enabled = asyncCfg.useReplQueue();
      if (enabled && interval > 0) {
         scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
               LogFactory.pushNDC(cacheName, trace);
               try {
                  flush();
               } finally {
                  LogFactory.popNDC(trace);
               }
            }
         }, interval, interval, TimeUnit.MILLISECONDS);
      }
   }

   /**
    * Stops the asynchronous flush queue.
    */
   @Override
   @Stop(priority = 9)
   // Stop before transport
   public void stop() {
      if (scheduledFuture != null) scheduledFuture.cancel(true);
      try {
         flush();
      } catch (Exception e) {
         log.debug("Unable to perform final flush before shutting down", e);
      }
      scheduledExecutor = null;
   }

   @Override
   public void add(ReplicableCommand job) {
      if (job == null)
         throw new NullPointerException("job is null");
      try {
         elements.put(job);
         if (elements.size() >= maxElements) flush();
      } catch (InterruptedException ie) {
         Thread.interrupted();
      }
   }

   @Override
   public synchronized int flush() {
      List<ReplicableCommand> toReplicate = drainReplQueue();
      if (trace) log.tracef("flush(): flushing repl queue (num elements=%s)", toReplicate.size());

      int toReplicateSize = toReplicate.size();
      if (toReplicateSize > 0) {
         try {
            log.tracef("Flushing %s elements", toReplicateSize);
            MultipleRpcCommand multipleRpcCommand = commandsFactory.buildReplicateCommand(toReplicate);
            // send to all live caches in the cluster
            //default rpc options
            rpcManager.invokeRemotely(null, multipleRpcCommand,
                                      rpcManager.getRpcOptionsBuilder(ResponseMode.getAsyncResponseMode(configuration))
                                            .skipReplicationQueue(true).build());
         } catch (Throwable t) {
            log.failedReplicatingQueue(toReplicate.size(), t);
         }
      }

      return toReplicateSize;
   }

   protected List<ReplicableCommand> drainReplQueue() {
      List<ReplicableCommand> toReplicate = new LinkedList<ReplicableCommand>();
      elements.drainTo(toReplicate);
      return toReplicate;
   }

   protected Configuration getConfiguration() {
      return configuration;
   }

   @Override
   public int getElementsCount() {
      return elements.size();
   }

   @Override
   public void reset() {
      elements.clear();
   }
}
