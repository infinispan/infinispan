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

package org.infinispan.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A simple <code>Future</code> implementation whose <code>get()</code> method blocks until another thread calls <code>set()</code>.
 * 
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 5.0
 */
public class ValueFuture<V> implements Future<V> {
   private CountDownLatch setLatch = new CountDownLatch(1);
   private V value;
   private Throwable exception;

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
   }

   @Override
   public boolean isCancelled() {
      return false;
   }

   @Override
   public boolean isDone() {
      return false;
   }

   @Override
   public V get() throws InterruptedException, ExecutionException {
      setLatch.await();
      if (exception != null)
         throw new ExecutionException(exception);
      return value;
   }

   @Override
   public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      setLatch.await(timeout, unit);
      if (exception != null)
         throw new ExecutionException(exception);
      return value;
   }

   public void set(V value) {
      this.value = value;
      setLatch.countDown();
   }

   public void setException(Throwable exception) {
      this.exception = exception;
      setLatch.countDown();
   }
}
