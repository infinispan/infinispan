package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationCallback;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.util.concurrent.CompletableFutures;

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

   public SimpleAsyncInvocationStage(Throwable throwable) {
      this.future = CompletableFutures.completedExceptionFuture(throwable);
   }

   @Override
   public Object get() throws Throwable {
      try {
         return CompletableFutures.await(future);
      } catch (ExecutionException e) {
         throw e.getCause();
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
   public Object addCallback(InvocationContext ctx, VisitableCommand command,
                        InvocationCallback function) {
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
            return new SimpleAsyncInvocationStage(t);
         }
      }
      return new QueueAsyncInvocationStage(ctx, command, future, function);
   }

   @Override
   public String toString() {
      return "SimpleAsyncInvocationStage(" + future + ')';
   }
}
