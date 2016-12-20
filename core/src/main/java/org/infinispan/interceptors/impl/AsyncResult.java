package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.InvocationStage;
import org.jgroups.annotations.GuardedBy;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * <p>It stores handler objects in a deque instead of creating a new instance every time a handler is added.
 * The deque may be frozen based on internal conditions, like executing the last handler or reaching the capacity
 * of the queue, and adding a handler will create a new instance.
 * </p>
 *
 * <p>When the queue is not frozen, adding a handler will change the result of the current stage.
 * When the queue is frozen, adding a handler may actually execute the handler synchronously.</p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
class AsyncResult extends CompletableFuture<Object> implements ComplexResult {
   final InvocationContext ctx;
   final VisitableCommand command;

   AsyncResult(InvocationContext ctx, VisitableCommand command) {
      this.ctx = ctx;
      this.command = command;
   }

   void completeFromStage(InvocationStage stage) {
      if (!stage.isDone())
         throw new IllegalArgumentException("Stage must be done");

      try {
         Object rv = stage.get();
         complete(rv);
      } catch (Throwable t) {
         completeExceptionally(t);
      }
   }

   void whenComplete(InvocationStageImpl stage) {
      if (addHandler(stage))
         return;

      super.whenComplete(stage);
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
   void freezeHandlers() {
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
   InvocationComposeHandler pollHandler() {
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
   int handlersSize() {
      synchronized (this) {
         // We can assume tail and head won't overflow
         return tail - head;
      }
   }
}
