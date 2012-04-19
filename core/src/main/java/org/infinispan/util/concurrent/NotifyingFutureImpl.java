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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Constructs an instance of a {@link org.infinispan.util.concurrent.NotifyingFuture}.
 * <p/>
 * Typical usage:
 * <p/>
 * <code> Object retval = .... // do some work here NotifyingFuture nf = new NotifyingFutureImpl(retval);
 * rpcManager.broadcastRpcCommandInFuture(nf, command); return nf; </code>
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class NotifyingFutureImpl implements NotifyingNotifiableFuture<Object> {

   final Object actualReturnValue;
   volatile Future<Object> ioFuture;
   //TODO revisit if volatile needed
   volatile boolean callCompleted = false;
   final Set<FutureListener<Object>> listeners = new CopyOnWriteArraySet<FutureListener<Object>>();
   final ReadWriteLock listenerLock = new ReentrantReadWriteLock();

   public NotifyingFutureImpl(Object actualReturnValue) {
      this.actualReturnValue = actualReturnValue;
   }

   @Override
   public void setNetworkFuture(Future<Object> future) {
      this.ioFuture = future;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return ioFuture.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return ioFuture.isCancelled();
   }

   @Override
   public boolean isDone() {
      return ioFuture.isDone();
   }

   @Override
   public Object get() throws InterruptedException, ExecutionException {
      if (!callCompleted) {
         ioFuture.get();
      }
      return actualReturnValue;
   }

   @Override
   public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      if (!callCompleted) {
         ioFuture.get(timeout, unit);
      }
      return actualReturnValue;
   }

   @Override
   public void notifyDone() {
      listenerLock.writeLock().lock();
      try {
         callCompleted = true;
         for (FutureListener<Object> l : listeners) l.futureDone(this);
      } finally {
         listenerLock.writeLock().unlock();
      }
   }

   @Override
   public NotifyingFuture<Object> attachListener(FutureListener<Object> objectFutureListener) {
      listenerLock.readLock().lock();
      try {
         if (!callCompleted) listeners.add(objectFutureListener);
         if (callCompleted) objectFutureListener.futureDone(this);
         return this;
      } finally {
         listenerLock.readLock().unlock();
      }
   }
}