package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

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
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * It id only meant to support the simplest asynchronous invocation, starting with
 * {@link org.infinispan.interceptors.BaseAsyncInterceptor#returnWithAsync(CompletableFuture)}
 * and only adding synchronous handlers on top of it.
 * Calling {@link #compose(InvocationComposeHandler)} or {@link #thenCompose(InvocationComposeSuccessHandler)}
 * will create a full {@link ComposedAsyncInvocationStage}.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class AsyncInvocationStage extends AbstractInvocationStage
      implements InvocationStage, BiFunction<Object, Throwable, Object> {
   private static final Log log = LogFactory.getLog(AsyncInvocationStage.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationComposeHandler handler;
   private CompletableFuture<Object> future;

   @SuppressWarnings("unchecked")
   public AsyncInvocationStage(InvocationContext ctx, VisitableCommand command, CompletionStage<?> future) {
      super(ctx, command);
      this.handler = null;
      this.future = (CompletableFuture<Object>) future;
   }

   private AsyncInvocationStage(InvocationContext ctx, VisitableCommand command, InvocationComposeHandler handler) {
      super(ctx, command);
      this.handler = handler;
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
   public InvocationStage compose(InvocationComposeHandler composeHandler) {
      return new ComposedAsyncInvocationStage(ctx, command, future.handle((rv, t) -> {
         AbstractInvocationStage stage;
         if (t == null) {
            stage = new ReturnValueStage(ctx, command, rv);
         } else {
            stage = new ExceptionStage(ctx, command, CompletableFutures.extractException(t));
         }
         return stage.compose(composeHandler);
      }));
   }

   @Override
   public InvocationStage thenCompose(InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(thenComposeHandler);
   }

   @Override
   public InvocationStage thenApply(InvocationReturnValueHandler returnValueHandler) {
      AsyncInvocationStage newStage = new AsyncInvocationStage(ctx, command, returnValueHandler);
      newStage.future = future.handle(newStage);
      return newStage;
   }

   @Override
   public InvocationStage thenAccept(InvocationSuccessHandler successHandler) {
      AsyncInvocationStage newStage = new AsyncInvocationStage(ctx, command, successHandler);
      newStage.future = future.handle(newStage);
      return newStage;
   }

   @Override
   public InvocationStage exceptionally(InvocationExceptionHandler exceptionHandler) {
      AsyncInvocationStage newStage = new AsyncInvocationStage(ctx, command, exceptionHandler);
      newStage.future = future.handle(newStage);
      return newStage;
   }

   @Override
   public InvocationStage handle(InvocationFinallyHandler finallyHandler) {
      AsyncInvocationStage newStage = new AsyncInvocationStage(ctx, command, finallyHandler);
      newStage.future = future.handle(newStage);
      return newStage;
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return future;
   }

   @Override
   public InvocationStage toInvocationStage(InvocationContext newCtx, VisitableCommand newCommand) {
      if (newCtx != ctx || newCommand != command) {
         return new AsyncInvocationStage(newCtx, newCommand, future);
      }
      return this;
   }

   @Override
   public Object apply(Object rv, Throwable t) {
      try {
         // apply() is only called if we have a handler
         if (trace) log.tracef("Executing invocation handler %s with command %s", Stages.className(handler), command);

         if (t == null) {
            if (handler instanceof InvocationFinallyHandler) {
               ((InvocationFinallyHandler) handler).accept(ctx, command, rv, null);
               return rv;
            } else if (handler instanceof InvocationSuccessHandler) {
               ((InvocationSuccessHandler) handler).accept(ctx, command, rv);
               return rv;
            } else if (handler instanceof InvocationReturnValueHandler) {
               return ((InvocationReturnValueHandler) handler).apply(ctx, command, rv);
            } else {
               // if (handler instanceof ExceptionStage)
               return rv;
            }
         } else {
            t = CompletableFutures.extractException(t);
            if (handler instanceof InvocationFinallyHandler) {
               ((InvocationFinallyHandler) handler).accept(ctx, command, null, t);
               throw t;
            } else if (handler instanceof InvocationExceptionHandler) {
               return ((InvocationExceptionHandler) handler).apply(ctx, command, t);
            } else {
               // if (handler instanceof InvocationReturnValueHandler | InvocationSuccessHandler)
               throw t;
            }
         }
      } catch (Throwable t1) {
         if (trace) log.trace("Exception from invocation handler", t1);
         throw CompletableFutures.asCompletionException(t1);
      }
   }
}
