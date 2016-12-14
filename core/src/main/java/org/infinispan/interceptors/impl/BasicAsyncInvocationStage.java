package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * It is only meant to support the simplest asynchronous invocation,
 * {@link org.infinispan.interceptors.BaseAsyncInterceptor#returnWithAsync(CompletableFuture)}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class BasicAsyncInvocationStage implements BasicInvocationStage {
   private final CompletableFuture<Object> future;

   @SuppressWarnings("unchecked")
   public BasicAsyncInvocationStage(CompletionStage<?> future) {
      this.future = (CompletableFuture<Object>) future;
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
   public InvocationStage toInvocationStage(InvocationContext newCtx, VisitableCommand newCommand) {
      return new AsyncInvocationStage(newCtx, newCommand, future);
   }
}
