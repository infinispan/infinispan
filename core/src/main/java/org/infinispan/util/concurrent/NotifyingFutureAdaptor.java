/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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

package org.infinispan.util.concurrent;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Receives a {@link Future} and exposes it as an {@link NotifyingFuture}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NotifyingFutureAdaptor<T> extends BaseNotifyingFuture {

   private static Log log = LogFactory.getLog(NotifyingFutureAdaptor.class);

   private volatile Future<T> actual;

   public void setActual(Future<T> actual) {
      this.actual = actual;
   }

   @Override
   public boolean cancel(boolean mayInterruptIfRunning) {
      return actual.cancel(mayInterruptIfRunning);
   }

   @Override
   public boolean isCancelled() {
      return actual.isCancelled();
   }

   @Override
   public boolean isDone() {
      return actual.isDone();
   }

   @Override
   public T get() throws InterruptedException, ExecutionException {
      T result = actual.get();
      log.tracef("Actual future completed with result %s", result);
      return result;
   }

   @Override
   public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return actual.get(timeout, unit);
   }
}
