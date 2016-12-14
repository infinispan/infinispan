package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BasicInvocationStage;
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
import org.jgroups.annotations.GuardedBy;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * <p>It stores handler objects in a deque instead of creating a new instance every time a handler is added.
 * The deque may be frozen based on internal conditions, like executing the last handler or reaching the capacity
 * of the queue, and adding a handler will create a new instance.
 * The queue will also be frozen when {@link #toCompletableFuture()} is invoked, to make that future behave like
 * a regular {@link CompletableFuture}.
 * </p>
 *
 * <p>When the queue is not frozen, adding a handler will change the result of the current stage.
 * When the queue is frozen, adding a handler may actually execute the handler synchronously.</p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class AsyncInvocationStage implements InvocationStage, InvocationComposeHandler, BiConsumer<Object, Throwable> {
   private static final Log log = LogFactory.getLog(AsyncInvocationStage.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationContext ctx;
   private final VisitableCommand command;
   private final CompletableFuture<Object> future = new CompletableFuture<>();

   public AsyncInvocationStage(InvocationContext ctx, VisitableCommand command,
                               CompletableFuture<?> valueFuture) {
      this.ctx = ctx;
      this.command = command;
      valueFuture.whenComplete(this);
   }

   @Override
   public Object get() throws Throwable {
      freezeHandlers();
      try {
         return CompletableFutures.await(future);
      } catch (ExecutionException e) {
         throw e.getCause();
      }
   }

   @Override
   public boolean isDone() {
      freezeHandlers();
      return future.isDone();
   }

   @Override
   public InvocationStage compose(InvocationComposeHandler composeHandler) {
      if (addHandler(composeHandler)) {
         return this;
      }

      // Deque is frozen
      InvocationStage stage;
      if (future.isDone()) {
         // We can switch to a synchronous stage
         try {
            Object rv = ((CompletableFuture<?>) future).getNow(null);
            stage = new ReturnValueStage(ctx, command, rv);
         } catch (Throwable t) {
            stage = new ExceptionStage(ctx, command, CompletableFutures.extractException(t));
         }
      } else {
         // Give up and create a new asynchronous stage
         stage = new AsyncInvocationStage(ctx, command, toCompletableFuture());
      }
      return stage.compose(composeHandler);
   }

   @Override
   public InvocationStage thenCompose(InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationSuccessHandler successHandler) {
      return compose(successHandler);
   }

   @Override
   public InvocationStage thenApply(InvocationReturnValueHandler returnValueHandler) {
      return compose(returnValueHandler);
   }

   @Override
   public InvocationStage exceptionally(InvocationExceptionHandler exceptionHandler) {
      return compose(exceptionHandler);
   }

   @Override
   public InvocationStage handle(InvocationFinallyHandler finallyHandler) {
      return compose(finallyHandler);
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      freezeHandlers();
      return future;
   }

   @Override
   public InvocationStage toInvocationStage(InvocationContext newCtx, VisitableCommand newCommand) {
      if (newCtx != ctx || newCommand != command) {
         freezeHandlers();
         return new AsyncInvocationStage(newCtx, newCommand, future);
      }
      return this;
   }

   @Override
   public void accept(Object rv, Throwable t) {
      // We started with a valueFuture, and that valueFuture is now complete.
      if (trace) log.tracef("Resuming invocation of command %s with %d handlers", command, handlersSize());
      InvocationStage currentStage;
      if (t != null) {
         currentStage = new ExceptionStage(ctx, command, CompletableFutures.extractException(t));
      } else {
         currentStage = new ReturnValueStage(ctx, command, rv);
      }
      invokeHandlers(currentStage);
   }

   @Override
   public BasicInvocationStage apply(BasicInvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand,
                                     Object rv, Throwable t) throws Throwable {
      // We ran a handler that returned another AsyncInvocationStage
      // and now that async stage is complete.
      invokeHandlers(stage.toInvocationStage(ctx, command));
      // We don't want to change the result of the other AsyncInvocationStage
      return stage;
   }

   private void invokeHandlers(InvocationStage currentStage) {
      while (true) {
         InvocationComposeHandler handler;
         handler = pollHandler();

         if (handler == null) {
            // Complete the future.
            // We finished running the handlers, and the last pollHandler() call locked the deque.
            completeFromStage(currentStage);
            return;
         }

         // Run the handler
         currentStage = currentStage.compose(handler);
         if (!currentStage.isDone()) {
            if (currentStage instanceof BasicAsyncInvocationStage) {
               // Use the CompletableFuture directly, without creating another AsyncInvocationStage instance
               currentStage.toCompletableFuture().whenComplete(this);
               return;
            } else if (currentStage instanceof AsyncInvocationStage) {
               AsyncInvocationStage asyncInvocationStage = (AsyncInvocationStage) currentStage;
               asyncInvocationStage.whenComplete(this);
               return;
            } else {
               currentStage = new ExceptionStage(ctx, command, new IllegalStateException(
                     "Unsupported asynchronous stage type: " + currentStage));
            }
         }

         // We got a synchronous invocation stage, continue with the next handler
      }
   }

   private void completeFromStage(BasicInvocationStage stage) {
      if (!stage.isDone())
         throw new IllegalArgumentException("Stage must be done");

      try {
         Object rv = stage.get();
         future.complete(rv);
      } catch (Throwable t) {
         future.completeExceptionally(t);
      }
   }

   private void whenComplete(AsyncInvocationStage otherStage) {
      if (addHandler(otherStage)) return;

      future.whenComplete(otherStage);
   }

   /**
    * An inline implementation of a deque with a fixed size that can be frozen so that no more elements
    * can be added.
    *
    * The deque is frozen manually, with {@link #freezeHandlers()}, or automatically when the last handler is polled,
    * or when the deque reaches capacity.
    */
   // Capacity must be a power of 2
   private static final int HANDLERS_DEQUE_CAPACITY = 8;
   @GuardedBy("this")
   private InvocationComposeHandler[] handlers = new InvocationComposeHandler[HANDLERS_DEQUE_CAPACITY];
   @GuardedBy("this")
   private byte mask = HANDLERS_DEQUE_CAPACITY - 1;
   @GuardedBy("this")
   private byte head;
   @GuardedBy("this")
   private byte tail;
   @GuardedBy("this")
   private boolean frozen;

   /**
    * Stop adding new handlers to this stage
    */
   private void freezeHandlers() {
      synchronized (this) {
         frozen = true;
      }
   }

   /**
    * Add a new handler to the deque.
    *
    * @return {@code true} if the handler was added, or {@code false} if the handlers deque is frozen
    */
   protected boolean addHandler(InvocationComposeHandler composeHandler) {
      synchronized (this) {
         if (frozen) {
            return false;
         }
         handlers[tail & mask] = composeHandler;
         tail++;
         if (((tail - head) & mask) == 0) {
            frozen = true;
         }
         return true;
      }
   }

   /**
    * Remove one handler from the deque, or freeze the deque if there are no more handlers.
    *
    * @return The next handler, or {@code null} if the deque is empty.
    */
   private InvocationComposeHandler pollHandler() {
      InvocationComposeHandler handler;
      synchronized (this) {
         if (tail != head) {
            handler = handlers[head & mask];
            head++;
         } else {
            handler = null;
            freezeHandlers();
         }
      }
      return handler;
   }

   /**
    * @return The current number of handlers in the deque, only useful for debugging.
    */
   private int handlersSize() {
      synchronized (this) {
         // We can assume tail and head won't overflow
         return tail - head;
      }
   }
}
