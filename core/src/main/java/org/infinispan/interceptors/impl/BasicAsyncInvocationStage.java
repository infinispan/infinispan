package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.InvocationComposeSuccessHandler;
import org.infinispan.interceptors.InvocationExceptionHandler;
import org.infinispan.interceptors.InvocationFinallyHandler;
import org.infinispan.interceptors.InvocationReturnValueHandler;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessHandler;
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
public class BasicAsyncInvocationStage implements InvocationStage {
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
   public InvocationStage compose(InvocationContext ctx, VisitableCommand command,
                                  InvocationComposeHandler composeHandler) {
      return new AsyncInvocationStage(ctx, command, future)
            .compose(ctx, command, composeHandler);
   }

   @Override
   public InvocationStage thenCompose(InvocationContext ctx, VisitableCommand command,
                                      InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(ctx, command, thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationContext ctx, VisitableCommand command,
                                     InvocationSuccessHandler successHandler) {
      return compose(ctx, command, successHandler);
   }

   @Override
   public InvocationStage thenApply(InvocationContext ctx, VisitableCommand command,
                                    InvocationReturnValueHandler returnValueHandler) {
      return compose(ctx, command, returnValueHandler);
   }

   @Override
   public InvocationStage exceptionally(InvocationContext ctx, VisitableCommand command,
                                        InvocationExceptionHandler exceptionHandler) {
      return compose(ctx, command, exceptionHandler);
   }

   @Override
   public InvocationStage handle(InvocationContext ctx, VisitableCommand command,
                                 InvocationFinallyHandler finallyHandler) {
      return compose(ctx, command, finallyHandler);
   }
}
