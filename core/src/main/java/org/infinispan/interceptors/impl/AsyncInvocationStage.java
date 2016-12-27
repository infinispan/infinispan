package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.InvocationComposeSuccessHandler;
import org.infinispan.interceptors.InvocationExceptionHandler;
import org.infinispan.interceptors.InvocationFinallyHandler;
import org.infinispan.interceptors.InvocationReturnValueHandler;
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
   public InvocationStage compose(InvocationContext ctx, VisitableCommand command, InvocationComposeHandler composeHandler) {
      if (ctx != this.ctx || command != this.command) {
         return new AsyncInvocationStage(ctx, command, future).compose(ctx, command, composeHandler);
      }

      if (addHandler(composeHandler)) {
         return this;
      }

      // Deque is frozen, that means we executed the last handler and we can continue synchronously
      InvocationStage stage;
      try {
         // join() instead of getNow() because the future is completed after freezing the deque
         Object rv = ((CompletableFuture<?>) future).join();
         stage = new ReturnValueStage(rv);
      } catch (Throwable t) {
         stage = new ExceptionStage(CompletableFutures.extractException(t));
      }
      return stage.compose(ctx, command, composeHandler);
   }

   @Override
   public InvocationStage thenCompose(InvocationContext ctx, VisitableCommand command, InvocationComposeSuccessHandler thenComposeHandler) {
      return compose(ctx, command, thenComposeHandler);
   }

   @Override
   public InvocationStage thenAccept(InvocationContext ctx, VisitableCommand command, InvocationSuccessHandler successHandler) {
      return compose(ctx, command, successHandler);
   }

   @Override
   public InvocationStage thenApply(InvocationContext ctx, VisitableCommand command, InvocationReturnValueHandler returnValueHandler) {
      return compose(ctx, command, returnValueHandler);
   }

   @Override
   public InvocationStage exceptionally(InvocationContext ctx, VisitableCommand command, InvocationExceptionHandler exceptionHandler) {
      return compose(ctx, command, exceptionHandler);
   }

   @Override
   public InvocationStage handle(InvocationContext ctx, VisitableCommand command, InvocationFinallyHandler finallyHandler) {
      return compose(ctx, command, finallyHandler);
   }

   @Override
   public CompletableFuture<Object> toCompletableFuture() {
      return future;
   }


   @Override
   public void accept(Object rv, Throwable t) {
      // We started with a valueFuture, and that valueFuture is now complete.
      if (trace) log.tracef("Resuming invocation of command %s with %d handlers", command, handlersSize());
      InvocationStage currentStage;
      if (t != null) {
         currentStage = new ExceptionStage(CompletableFutures.extractException(t));
      } else {
         currentStage = new ReturnValueStage(rv);
      }
      invokeHandlers(currentStage);
   }

   @Override
   public InvocationStage apply(InvocationStage stage, InvocationContext rCtx, VisitableCommand rCommand,
                                Object rv, Throwable t) throws Throwable {
      // We ran a handler that returned another AsyncInvocationStage
      // and now that async stage is complete.
      invokeHandlers(stage);
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

         // Run the handler (catching any exception and turning it into an ExceptionStage)
         currentStage = currentStage.compose(ctx, command, handler);
         if (!currentStage.isDone()) {
            if (currentStage instanceof BasicAsyncInvocationStage) {
               // Use the CompletableFuture directly, without creating another AsyncInvocationStage instance
               currentStage.toCompletableFuture().whenComplete(this);
               return;
            } else if (currentStage instanceof AsyncInvocationStage) {
               AsyncInvocationStage asyncInvocationStage = (AsyncInvocationStage) currentStage;
               if (asyncInvocationStage.addHandler(this)) {
                  // Stop invoking handlers now and resume when the other stage is complete.
                  return;
               }
               // Deque is frozen, we can continue with the next iteration
            } else {
               currentStage = new ExceptionStage(new IllegalStateException(
                     "Unsupported asynchronous stage type: " + currentStage));
            }
         }

         // We got a synchronous invocation stage, continue with the next handler
      }
   }

   private void completeFromStage(InvocationStage stage) {
      if (!stage.isDone())
         throw new IllegalArgumentException("Stage must be done");

      try {
         Object rv = stage.get();
         future.complete(rv);
      } catch (Throwable t) {
         future.completeExceptionally(t);
      }
   }

   /**
    * An inline implementation of a deque with a fixed size that can be frozen so that no more elements
    * can be added.
    *
    * The deque is frozen automatically when the last handler is polled.
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
   boolean addHandler(InvocationComposeHandler composeHandler) {
      synchronized (this) {
         if (frozen) {
            return false;
         }
         if (handlers == null) {
            initDeque();
         }
         handlers[tail & mask] = composeHandler;
         tail++;
         if (((tail - head) & mask) == 0) {
            expandDeque();
         }
         return true;
      }
   }

   @GuardedBy("this")
   private void initDeque() {
      handlers = new InvocationComposeHandler[HANDLERS_DEQUE_CAPACITY];
      mask = HANDLERS_DEQUE_CAPACITY - 1;
   }

   @GuardedBy("this")
   private void expandDeque() {
      InvocationComposeHandler[] oldHandlers = handlers;
      int oldMask = mask;
      int oldSize = oldMask + 1;

      int newSize = oldSize * 2;
      int newMask = newSize - 1;
      handlers = new InvocationComposeHandler[newSize];
      mask = (byte) newMask;

      int maskedHead = head & mask;
      System.arraycopy(oldHandlers, maskedHead, handlers, 0, oldSize - maskedHead);
      System.arraycopy(oldHandlers, 0, handlers, maskedHead, maskedHead);
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
