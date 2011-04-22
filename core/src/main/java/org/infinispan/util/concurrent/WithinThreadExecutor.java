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
package org.infinispan.util.concurrent;

import org.infinispan.CacheException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An executor that works within the current thread.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @see <a href="http://jcip.net/">Java Concurrency In Practice</a>
 * @since 4.0
 */
public class WithinThreadExecutor implements ExecutorService {
   boolean shutDown = false;

   public void execute(Runnable command) {
      command.run();
   }

   public void shutdown() {
      shutDown = true;
   }

   public List<Runnable> shutdownNow() {
      shutDown = true;
      return Collections.emptyList();
   }

   public boolean isShutdown() {
      return shutDown;
   }

   public boolean isTerminated() {
      return shutDown;
   }

   public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return shutDown;
   }

   public <T> Future<T> submit(Callable<T> task) {
      try {
         final T resp = task.call();
         return new Future<T>() {

            public boolean cancel(boolean mayInterruptIfRunning) {
               return false;
            }

            public boolean isCancelled() {
               return false;
            }

            public boolean isDone() {
               return true;
            }

            public T get() throws InterruptedException, ExecutionException {
               return resp;
            }

            public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
               return resp;
            }
         };
      }
      catch (Exception e) {
         throw new CacheException(e);
      }
   }

   public <T> Future<T> submit(Runnable task, T result) {
      throw new UnsupportedOperationException();
   }

   public Future<?> submit(Runnable task) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   // unchecked on purpose due to http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
   public List invokeAll(Collection tasks) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   // unchecked on purpose due to http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
   public List invokeAll(Collection tasks, long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   // unchecked on purpose due to http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
   public Object invokeAny(Collection tasks) throws InterruptedException, ExecutionException {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("unchecked")
   // unchecked on purpose due to http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6267833
   public Object invokeAny(Collection tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException();
   }
}
