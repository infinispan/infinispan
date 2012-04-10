/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import java.util.concurrent.*;

/**
 * This is a notifying and notifiable future whose return value is not known
 * at construction time. Instead, the return value comes from the result of
 * the operation called in the Callable or Runnable.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class DeferredReturnFuture<V> implements NotifyingNotifiableFuture<V> {

   private final NotifyingFutureImpl delegateFuture = new NotifyingFutureImpl(null);

   @Override
   public V get() throws InterruptedException, ExecutionException {
      // Return the network's future result
      return (V) delegateFuture.ioFuture.get();
   }

   @Override
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      return (V) delegateFuture.ioFuture.get(timeout, unit);
   }

   @Override
   public void notifyDone() {
      delegateFuture.notifyDone();
   }

   @Override
   public void setNetworkFuture(Future<V> future) {
      delegateFuture.setNetworkFuture((Future<Object>) future);
   }

   @Override
   public NotifyingFuture<V> attachListener(FutureListener<V> objectFutureListener) {
      return (NotifyingFuture<V>) delegateFuture.attachListener((FutureListener<Object>) objectFutureListener);
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return delegateFuture.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return delegateFuture.isCancelled();
   }

   @Override
   public boolean isDone() {
      return delegateFuture.isDone();
   }

}
