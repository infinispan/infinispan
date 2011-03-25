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
package org.infinispan.distexec;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

/**
 * 
 */
public class DistributedRunnableFuture<V> implements RunnableFuture<V>, NotifyingNotifiableFuture<V> {

   protected final DistributedExecuteCommand<V> sync;
   protected volatile Future<V> f;

   /**
    * Creates a <tt>DistributedRunnableFuture</tt> that will upon running, execute the given
    * <tt>Runnable</tt>, and arrange that <tt>get</tt> will return the given result on successful
    * completion.
    * 
    * 
    * @param runnable
    *           the runnable task
    * @param result
    *           the result to return on successful completion.
    * @throws NullPointerException
    *            if runnable is null
    */
   public DistributedRunnableFuture(DistributedExecuteCommand<V> command) {
      this.sync = command;
   }

   public DistributedExecuteCommand<V> getCommand() {
      return sync;
   }

   public boolean isCancelled() {
      return sync.isCancelled();
   }

   public boolean isDone() {
      return sync.isDone();
   }

   public boolean cancel(boolean mayInterruptIfRunning) {
      return sync.cancel(mayInterruptIfRunning);
   }

   /**
    * 
    */
   public V get() throws InterruptedException, ExecutionException {
      V v = sync.get();
      return v;
   }

   /**
    * 
    */
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
      V v = sync.get(unit.toNanos(timeout));
      return v;
   }

   @Override
   public void run() {
      sync.innerRun();
   }

   @Override
   public void notifyDone() {
      Future<V> future = f;
      sync.setWithFuture(future);
   }

   @Override
   public NotifyingFuture<V> attachListener(FutureListener<V> listener) {
      return this;
   }

   @Override
   public void setNetworkFuture(Future<V> future) {
      this.f = future;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (!(o instanceof DistributedRunnableFuture)) {
         return false;
      }

      DistributedRunnableFuture<?> that = (DistributedRunnableFuture<?>) o;
      return that.getCommand().equals(getCommand());
   }

   @Override
   public int hashCode() {
      return getCommand().hashCode();
   }

   /**
    * This is copied from {@see java.util.concurrent.Executors} class which contains
    * RunnableAdapter. However that adapter isn't serializable, and is final and package level so we
    * can' reference.
    */
   protected static final class RunnableAdapter<T> implements Callable<T>, Serializable {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1504333583232160873L;

      protected Runnable task;
      protected T result;

      protected RunnableAdapter() {
      }

      protected RunnableAdapter(Runnable task, T result) {
         this.task = task;
         this.result = result;
      }

      public T call() {
         task.run();
         return result;
      }
   }
}