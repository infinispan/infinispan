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
package org.infinispan.remoting;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.config.Configuration;
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
import java.util.concurrent.TimeUnit;

/**
 * Periodically (or when certain size is exceeded) takes elements and replicates them.
 *
 * @author <a href="mailto:bela@jboss.org">Bela Ban</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ReplicationQueue {
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

   public boolean isEnabled() {
      return enabled;
   }

   @Inject
   private void injectDependencies(@ComponentName(KnownComponentNames.ASYNC_REPLICATION_QUEUE_EXECUTOR) ScheduledExecutorService executor,
                                   RpcManager rpcManager, Configuration configuration, CommandsFactory commandsFactory) {
      this.rpcManager = rpcManager;
      this.configuration = configuration;
      this.commandsFactory = commandsFactory;
      this.scheduledExecutor = executor;
   }

   /**
    * Starts the asynchronous flush queue.
    */
   @Start
   public void start() {
      long interval = configuration.getReplQueueInterval();
      log.trace("Starting replication queue, with interval {0} and maxElements {1}", interval, maxElements);
      this.maxElements = configuration.getReplQueueMaxElements();
      // check again
      enabled = configuration.isUseReplQueue();
      if (enabled && interval > 0) {
         scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
               flush();
            }
         }, interval, interval, TimeUnit.MILLISECONDS);
      }
   }

   /**
    * Stops the asynchronous flush queue.
    */
   @Stop(priority = 9) // Stop before transport
   public void stop() {
      if (scheduledExecutor != null) {
         scheduledExecutor.shutdownNow();
      }
      scheduledExecutor = null;
   }


   /**
    * Adds a new method call.
    */
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

   /**
    * Flushes existing method calls.
    */
   public void flush() {
      List<ReplicableCommand> toReplicate = new LinkedList<ReplicableCommand>();
      elements.drainTo(toReplicate);
      if (log.isTraceEnabled()) log.trace("flush(): flushing repl queue (num elements={0})", toReplicate.size());

      int toReplicateSize = toReplicate.size();
      if (toReplicateSize > 0) {
         try {
            log.trace("Flushing {0} elements", toReplicateSize);
            MultipleRpcCommand multipleRpcCommand = commandsFactory.buildReplicateCommand(toReplicate);
            // send to all live caches in the cluster
            rpcManager.invokeRemotely(null, multipleRpcCommand, ResponseMode.getAsyncResponseMode(configuration), configuration.getSyncReplTimeout());
         }
         catch (Throwable t) {
            log.error("failed replicating " + toReplicate.size() + " elements in replication queue", t);
         }
      }
   }

   public int getElementsCount() {
      return elements.size();
   }

   public void reset() {
      elements.clear();
   }
}
