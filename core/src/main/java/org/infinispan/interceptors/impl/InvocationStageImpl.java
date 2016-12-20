package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

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
 * Invocation stage representing a computation that already completed successfully.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class InvocationStageImpl implements InvocationStage, InvocationComposeHandler, BiConsumer<Object, Throwable> {
   private static final Log log = LogFactory.getLog(InvocationStageImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Object result;

   public static InvocationStageImpl makeSuccessful(Object returnValue) {
      return new InvocationStageImpl(returnValue);
   }

   public static InvocationStageImpl makeExceptional(Throwable throwable) {
      return new InvocationStageImpl(AsyncResult.makeExceptional(null, null, CompletableFutures.extractException(throwable)));
   }

   private static InvocationStageImpl makeSynchronous(Object returnValue, Throwable throwable) {
      if (throwable == null) {
         return makeSuccessful(returnValue);
      } else {
         return makeExceptional(throwable);
      }
   }

   public static InvocationStageImpl makeAsynchronous(InvocationContext ctx, VisitableCommand command, CompletableFuture<?> completableFuture) {
      InvocationStageImpl stage = new InvocationStageImpl(new AsyncResult(ctx, command));
      completableFuture.whenComplete(stage);
      return stage;
   }

   private InvocationStageImpl(Object result) {
      this.result = result;
   }

   @Override
   public InvocationStage compose(InvocationContext ctx, VisitableCommand command,
                                  InvocationComposeHandler composeHandler) {
      return doCompose(ctx, command, composeHandler, true);
   }

   @Override
   public InvocationStage thenCompose(InvocationContext ctx, VisitableCommand command,
                                      InvocationComposeSuccessHandler thenComposeHandler) {
      return doCompose(ctx, command, thenComposeHandler, false);
   }

   private InvocationStage doCompose(InvocationContext ctx, VisitableCommand command,
                                     InvocationComposeHandler thenComposeHandler, boolean acceptExceptions) {
      try {
         if (result instanceof AsyncResult) {
            return composeWithAsyncResult(ctx, command, thenComposeHandler);
         } else {
            return thenComposeHandler.apply(this, ctx, command, result, null);
         }
      } catch (Throwable t) {
         this.result = AsyncResult.makeExceptional(ctx, command, t);
         return this;
      }
   }

   private InvocationStage composeWithAsyncResult(InvocationContext ctx, VisitableCommand command,
                                                  InvocationComposeHandler composeHandler) throws Throwable {
      AsyncResult asyncResult = (AsyncResult) this.result;
      if (asyncResult.addHandler(composeHandler)) {
         return this;
      }

      // Deque is frozen
      return composeAsyncResultWithFrozenQueue(ctx, command, composeHandler, asyncResult);
   }

   private InvocationStage composeAsyncResultWithFrozenQueue(InvocationContext ctx, VisitableCommand command,
                                                             InvocationComposeHandler composeHandler,
                                                             AsyncResult asyncResult) throws Throwable {
      if (asyncResult.isDone()) {
         // We can switch to a synchronous result
         Object rv = null;
         Throwable throwable = null;
         try {
            rv = asyncResult.getNow(null);
         } catch (Throwable t) {
            throwable = CompletableFutures.extractException(t);
         }
         // The caller will catch any exception and replace the current stage's result
         return composeHandler.apply(this, ctx, command, rv, throwable);
      } else {
         // Create a new asynchronous stage with an empty deque, and retry there
         InvocationStageImpl newStage = makeAsynchronous(ctx, command, asyncResult);
         return newStage.composeWithAsyncResult(ctx, command, composeHandler);
      }
   }

   @Override
   public InvocationStage thenAccept(InvocationContext ctx, VisitableCommand command, InvocationSuccessHandler successHandler) {
      try {
         if (result instanceof AsyncResult) {
            return composeWithAsyncResult(ctx, command, successHandler);
         } else {
            successHandler.accept(ctx, command, result);
            return this;
         }
      } catch (Throwable t) {
         this.result = AsyncResult.makeExceptional(ctx, command, t);
         return this;
      }
   }

   @Override
   public InvocationStage thenApply(InvocationContext ctx, VisitableCommand command, InvocationReturnValueHandler returnValueHandler) {
      try {
         if (result instanceof AsyncResult) {
            return composeWithAsyncResult(ctx, command, returnValueHandler);
         } else {
            this.result = returnValueHandler.apply(ctx, command, result);
            return this;
         }
      } catch (Throwable t) {
         this.result = AsyncResult.makeExceptional(ctx, command, t);
         return this;
      }
   }

   @Override
   public InvocationStage exceptionally(InvocationContext ctx, VisitableCommand command, InvocationExceptionHandler exceptionHandler) {
      try {
         if (result instanceof AsyncResult) {
            return composeWithAsyncResult(ctx, command, exceptionHandler);
         } else {
            // No exception
            return this;
         }
      } catch (Throwable t) {
         this.result = AsyncResult.makeExceptional(ctx, command, t);
         return this;
      }
   }

   @Override
   public InvocationStage handle(InvocationContext ctx, VisitableCommand command,
                                 InvocationFinallyHandler finallyHandler) {
      try {
         if (result instanceof AsyncResult) {
            return composeWithAsyncResult(ctx, command, finallyHandler);
         } else {
            finallyHandler.accept(ctx, command, result, null);
            return this;
         }
      } catch (Throwable t) {
         this.result = AsyncResult.makeExceptional(ctx, command, t);
         return this;
      }
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      if (result instanceof AsyncResult) {
         return ((AsyncResult) result);
      } else {
         return CompletableFuture.completedFuture(result);
      }
   }

   @Override
   public Object get() throws Throwable {
      if (!(result instanceof AsyncResult)) {
         return result;
      } else {
         try {
            return ((AsyncResult) result).get();
         } catch (ExecutionException e) {
            throw e.getCause();
         }
      }
   }

   @Override
   public boolean isDone() {
      return !(result instanceof AsyncResult) || ((AsyncResult) result).isDone();
   }

   @Override
   public String toString() {
      return "InvocationStageImpl(" + Stages.className(result) + ")";
   }

   @Override
   public void accept(Object o, Throwable throwable) {
      // We started with a valueFuture, and that valueFuture is now complete.
      invokeHandlers(makeSynchronous(o, throwable));
   }

   private void invokeHandlers(InvocationStageImpl currentStage) {
      AsyncResult asyncResult = (AsyncResult) this.result;
      if (trace) log.tracef("Resuming invocation of command %s with %d handlers", asyncResult.command, asyncResult.handlersSize());
      while (true) {
         InvocationComposeHandler handler;
         handler = asyncResult.pollHandler();

         if (handler == null) {
            // Complete the future.
            // We finished running the handlers, and the last pollHandler() call locked the deque.
            asyncResult.completeFromStage(currentStage);
            return;
         }

         // Run the handler
         currentStage = (InvocationStageImpl) currentStage.compose(asyncResult.ctx, asyncResult.command, handler);
         if (!currentStage.isDone()) {
            // The stage is not completed, that means the result is an AsyncResult.
            // We stop running more handlers and continue only that AsyncResult is done.
            AsyncResult newAsyncResult = (AsyncResult) currentStage.result;
            newAsyncResult.whenComplete(this);
            return;
         }
      }
   }

   @Override
   public InvocationStage apply(InvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand,
                                     Object rv, Throwable t) throws Throwable {
      // We ran a handler that returned another AsyncInvocationStage
      // and now that async stage is complete.
      invokeHandlers((InvocationStageImpl) stage);
      return stage;
   }
}
