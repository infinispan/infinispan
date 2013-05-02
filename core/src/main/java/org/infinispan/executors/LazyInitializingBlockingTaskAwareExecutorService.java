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
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.executors;

import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorServiceImpl;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A delegating executor that lazily constructs and initializes the underlying executor.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public final class LazyInitializingBlockingTaskAwareExecutorService implements BlockingTaskAwareExecutorService {

   private final ExecutorFactory factory;
   private final Properties executorProperties;
   private final TimeService timeService;
   private volatile BlockingTaskAwareExecutorService delegate;

   public LazyInitializingBlockingTaskAwareExecutorService(ExecutorFactory factory, Properties executorProperties,
                                                           TimeService timeService) {
      this.factory = factory;
      this.executorProperties = executorProperties;
      this.timeService = timeService;
   }

   @Override
   public void execute(BlockingRunnable runnable) {
      initIfNeeded();
      delegate.execute(runnable);
   }

   @Override
   public void checkForReadyTasks() {
      if (delegate != null) {
         delegate.checkForReadyTasks();
      }
   }

   @Override
   public void shutdown() {
      if (delegate != null) delegate.shutdown();
   }

   @Override
   public List<Runnable> shutdownNow() {
      if (delegate == null)
         return InfinispanCollections.emptyList();
      else
         return delegate.shutdownNow();
   }

   @Override
   public boolean isShutdown() {
      return delegate == null || delegate.isShutdown();
   }

   @Override
   public boolean isTerminated() {
      return delegate == null || delegate.isTerminated();
   }

   @Override
   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      if (delegate == null)
         return true;
      else
         return delegate.awaitTermination(timeout, unit);
   }

   @Override
   public <T> Future<T> submit(Callable<T> task) {
      initIfNeeded();
      return delegate.submit(task);
   }

   @Override
   public <T> Future<T> submit(Runnable task, T result) {
      initIfNeeded();
      return delegate.submit(task, result);
   }

   @Override
   public Future<?> submit(Runnable task) {
      initIfNeeded();
      return delegate.submit(task);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      initIfNeeded();
      return delegate.invokeAll(tasks);
   }

   @Override
   public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      initIfNeeded();
      return delegate.invokeAll(tasks, timeout, unit);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      initIfNeeded();
      return delegate.invokeAny(tasks);
   }

   @Override
   public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      initIfNeeded();
      return delegate.invokeAny(tasks, timeout, unit);
   }

   @Override
   public void execute(Runnable command) {
      initIfNeeded();
      delegate.execute(command);
   }

   private void initIfNeeded() {
      if (delegate == null) {
         synchronized (this) {
            if (delegate == null) {
               delegate = new BlockingTaskAwareExecutorServiceImpl(factory.getExecutor(executorProperties), timeService);
            }
         }
      }
   }
}
