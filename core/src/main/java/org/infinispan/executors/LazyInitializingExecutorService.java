/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.executors;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A delegating executor that lazily constructs and initalizes the underlying executor, since unused JDK executors
 * are expensive.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class LazyInitializingExecutorService implements ExecutorService {
   private ExecutorService delegate;
   private final ExecutorFactory factory;
   private final Properties executorProperties;

   public LazyInitializingExecutorService(ExecutorFactory factory, Properties executorProperties) {
      this.factory = factory;
      this.executorProperties = executorProperties;
   }

   private void initIfNeeded() {
      if (delegate == null) {
         synchronized (this) {
            if (delegate == null) {
               delegate = factory.getExecutor(executorProperties);
            }
         }
      }
   }

   @Override
   public void shutdown() {
      if (delegate != null) delegate.shutdown(); 
   }

   @Override
   public List<Runnable> shutdownNow() {
      if (delegate == null)
         return Collections.emptyList();
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
}
