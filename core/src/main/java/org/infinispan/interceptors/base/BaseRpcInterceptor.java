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
package org.infinispan.interceptors.base;

import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.CacheRpcManager;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Acts as a base for all RPC calls - subclassed by
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class BaseRpcInterceptor extends CommandInterceptor {

   protected CacheRpcManager rpcManager;
   protected ExecutorService asyncExecutorService;

   @Inject
   public void init(CacheRpcManager rpcManager,
                    @ComponentName(KnownComponentNames.ASYNC_SERIALIZATION_EXECUTOR) ExecutorService e) {
      this.rpcManager = rpcManager;
      this.asyncExecutorService = e;
   }

   protected boolean defaultSynchronous;

   @Start
   public void init() {
      defaultSynchronous = configuration.getCacheMode().isSynchronous();
   }

   protected final boolean isSynchronous(InvocationContext ctx) {
      if (ctx.hasFlag(Flag.FORCE_SYNCHRONOUS))
         return true;
      else if (ctx.hasFlag(Flag.FORCE_ASYNCHRONOUS))
         return false;

      return defaultSynchronous;
   }

   protected final boolean isLocalModeForced(InvocationContext ctx) {
      if (ctx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         if (trace) log.debug("LOCAL mode forced on invocation.  Suppressing clustered events.");
         return true;
      }
      return false;
   }

   protected final <X> Future<X> submitRpcCall(Callable<Object> c, final Object returnValue) {
      final Future f = asyncExecutorService.submit(c);
      return new Future<X>() {

         public boolean cancel(boolean mayInterruptIfRunning) {
            return f.cancel(mayInterruptIfRunning);
         }

         public boolean isCancelled() {
            return f.isCancelled();
         }

         public boolean isDone() {
            return f.isDone();
         }

         @SuppressWarnings("unchecked")
         public X get() throws InterruptedException, ExecutionException {
            f.get(); // wait for f to complete first
            return (X) returnValue;
         }

         @SuppressWarnings("unchecked")
         public X get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            f.get(timeout, unit);
            return (X) returnValue;
         }
      };
   }

}