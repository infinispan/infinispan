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

package org.infinispan.util.concurrent;

import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;

/**
 * A future that doesn't do anything and simply returns a given return value.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class NoOpFuture<E> implements NotifyingNotifiableFuture<E> {
   private final E returnValue;

   public NoOpFuture(E returnValue) {
      this.returnValue = returnValue;
   }

   @Override
   public boolean cancel(boolean b) {
      return false;
   }

   @Override
   public boolean isCancelled() {
      return false;
   }

   @Override
   public boolean isDone() {
      return true;
   }

   @Override
   public E get() throws InterruptedException, ExecutionException {
      return returnValue;
   }

   @Override
   public E get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
      return returnValue;
   }

   @Override
   public void notifyDone() {      
   }

   @Override
   public void setNetworkFuture(Future<E> eFuture) {
      throw new UnsupportedOperationException();
   }

   @Override
   public NotifyingFuture<E> attachListener(FutureListener<E> eFutureListener) {
      eFutureListener.futureDone(this);
      return this;
   }
}
