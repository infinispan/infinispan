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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * For use with > 1 underlying network future
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class AggregatingNotifyingFutureImpl extends NotifyingFutureImpl {
   final List<Future<Object>> futures;
   final AtomicInteger awaitingCompletions = new AtomicInteger();

   public AggregatingNotifyingFutureImpl(Object actualReturnValue, int maxFutures) {
      super(actualReturnValue);
      futures = new ArrayList<Future<Object>>(maxFutures);
      awaitingCompletions.set(maxFutures);
   }

   @Override
   public void setNetworkFuture(Future<Object> future) {
      futures.add(future);
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      boolean aggregateValue = false;
      for (Future<Object> f : futures) aggregateValue = f.cancel(mayInterruptIfRunning) && aggregateValue;
      return aggregateValue;
   }

   @Override
   public boolean isCancelled() {
      for (Future<Object> f : futures) if (f.isCancelled()) return true;
      return false;
   }

   @Override
   public boolean isDone() {
      for (Future<Object> f : futures) if (!f.isDone()) return false;
      return true;
   }

   @Override
   public Object get() throws InterruptedException, ExecutionException {
      for (Future<Object> f : futures) f.get();
      return actualReturnValue;
   }

   @Override
   public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, java.util.concurrent.TimeoutException {
      for (Future<Object> f : futures) f.get(timeout, unit);
      return actualReturnValue;
   }

   @Override
   public void notifyDone() {
      if (awaitingCompletions.decrementAndGet() == 0) super.notifyDone();
   }
}
