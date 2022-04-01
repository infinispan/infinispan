package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.ExceptionSyncInvocationStage;
import org.infinispan.interceptors.InvocationCallback;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.logging.TraceException;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * It is only meant to support the simplest asynchronous invocation,
 * {@link org.infinispan.interceptors.BaseAsyncInterceptor#asyncValue(CompletionStage)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class SimpleAsyncInvocationStage extends InvocationStage {
   protected final CompletableFuture<Object> future;

   @SuppressWarnings("unchecked")
   public SimpleAsyncInvocationStage(CompletionStage<?> future) {
      this.future = (CompletableFuture<Object>) future;
   }

   @Override
   public Object get() throws Throwable {
      try {
         return CompletableFutures.await(future);
      } catch (ExecutionException e) {
         Throwable cause = e.getCause();
         cause.addSuppressed(new TraceException());
         throw cause;
      }
   }

   @Override
   public boolean isDone() {
      return future.isDone();
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return future;
   }

   @Override
   public <C extends VisitableCommand> Object addCallback(InvocationContext ctx, C command,
                        InvocationCallback<C> function) {
      if (future.isDone()) {
         Object rv;
         Throwable throwable;
         try {
            rv = future.getNow(null);
            throwable = null;
         } catch (Throwable t) {
            rv = null;
            throwable = CompletableFutures.extractException(t);
         }
         try {
            return function.apply(ctx, command, rv, throwable);
         } catch (Throwable t) {
            return new ExceptionSyncInvocationStage(t);
         }
      }
      return new QueueAsyncInvocationStage(ctx, command, future, function);
   }

   @Override
   public Object thenReturn(InvocationContext ctx, VisitableCommand command, Object returnValue) {
      if (future.isDone()) {
         return future.isCompletedExceptionally() ? this : returnValue;
      }
      return new QueueAsyncInvocationStage(ctx, command, future,
            (InvocationSuccessFunction) (rCtx, rCommand, rv) -> returnValue);
   }

   @Override
   public String toString() {
      return "SimpleAsyncInvocationStage(" + future + ')';
   }
}
