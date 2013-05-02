/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.util.concurrent;

import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A special executor service that accepts a {@code BlockingRunnable}. This special runnable gives hints about the code
 * to be running in order to avoiding put a runnable that will block the thread. In this way, only when the runnable
 * says that is ready, it is sent to the real executor service
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class BlockingTaskAwareExecutorServiceImpl extends AbstractExecutorService implements BlockingTaskAwareExecutorService {

   private static final Log log = LogFactory.getLog(BlockingTaskAwareExecutorServiceImpl.class);
   private final BlockingQueue<BlockingRunnable> blockedTasks;
   private final ExecutorService executorService;
   private final TimeService timeService;
   private volatile boolean shutdown;

   public BlockingTaskAwareExecutorServiceImpl(ExecutorService executorService, TimeService timeService) {
      this.blockedTasks = new LinkedBlockingQueue<BlockingRunnable>();
      this.executorService = executorService;
      this.timeService = timeService;
      this.shutdown = false;
   }

   @Override
   public final void execute(BlockingRunnable runnable) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      if (runnable.isReady()) {
         doExecute(runnable);
      } else {
         blockedTasks.offer(runnable);
      }
      if (log.isTraceEnabled()) {
         log.tracef("Added a new task: %s task(s) are waiting", blockedTasks.size());
      }
   }

   @Override
   public void shutdown() {
      shutdown = true;
   }

   @Override
   public List<Runnable> shutdownNow() {
      shutdown = true;
      List<Runnable> runnableList = new LinkedList<Runnable>();
      runnableList.addAll(executorService.shutdownNow());
      runnableList.addAll(blockedTasks);
      return runnableList;
   }

   @Override
   public boolean isShutdown() {
      return shutdown;
   }

   @Override
   public boolean isTerminated() {
      return shutdown && blockedTasks.isEmpty() && executorService.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      final long endTime = timeService.expectedEndTime(timeout, unit);
      synchronized (blockedTasks) {
         long waitTime = timeService.remainingTime(endTime, TimeUnit.MILLISECONDS);
         while (!blockedTasks.isEmpty() && waitTime > 0) {
            wait(waitTime);
         }
      }
      return isTerminated();
   }

   @Override
   public final void checkForReadyTasks() {
      List<BlockingRunnable> runnableReadyList = new ArrayList<BlockingRunnable>(blockedTasks.size());
      synchronized (blockedTasks) {
         for (Iterator<BlockingRunnable> iterator = blockedTasks.iterator(); iterator.hasNext(); ) {
            BlockingRunnable runnable = iterator.next();
            if (runnable.isReady()) {
               iterator.remove();
               runnableReadyList.add(runnable);
            }
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("Tasks executed=%s, still pending=%s", runnableReadyList.size(), blockedTasks.size());
      }

      for (BlockingRunnable runnable : runnableReadyList) {
         doExecute(runnable);
      }
   }

   @Override
   public void execute(Runnable command) {
      if (shutdown) {
         throw new RejectedExecutionException("Executor Service is already shutdown");
      }
      executorService.execute(command);
   }

   private void doExecute(BlockingRunnable runnable) {
      try {
         executorService.execute(runnable);
      } catch (RejectedExecutionException rejected) {
         //put it back!
         blockedTasks.offer(runnable);
      }
   }
}
