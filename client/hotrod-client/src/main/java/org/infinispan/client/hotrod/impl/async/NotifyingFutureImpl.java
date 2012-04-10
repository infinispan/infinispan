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
package org.infinispan.client.hotrod.impl.async;

import org.infinispan.util.concurrent.FutureListener;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Notifying future implementation for async calls.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NotifyingFutureImpl<T> implements NotifyingFuture<T> {

   private volatile Future<T> executing;
   private volatile CopyOnWriteArraySet<FutureListener<T>> listeners;

   public void setExecuting(Future<T> executing) {
      this.executing = executing;
   }

   @Override
   public NotifyingFuture<T> attachListener(FutureListener<T> futureListener) {
      if (listeners == null) {
         listeners = new CopyOnWriteArraySet<FutureListener<T>>();
      }
      listeners.add(futureListener);
      return this;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      try {
         return executing.cancel(mayInterruptIfRunning);
      } finally {
         notifyFutureCompletion();
      }
   }

   public void notifyFutureCompletion() {
      if (listeners != null) {
         for (FutureListener<T> listener : listeners) {
            listener.futureDone(this);
         }
      }
   }

   @Override
   public boolean isCancelled() {
      return executing.isCancelled();
   }

   @Override
   public boolean isDone() {
      return executing.isDone();
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      return executing.get();
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return executing.get(timeout, unit);
   }
}
