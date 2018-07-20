package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.InvocationCallback;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.annotations.GuardedBy;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * <p>It stores handler objects in a queue instead of creating a new instance every time a handler is added.
 * The queue may be frozen based on internal conditions, like executing the last handler or reaching the capacity
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
public class QueueAsyncInvocationStage extends SimpleAsyncInvocationStage implements BiConsumer<Object, Throwable>,
      InvocationCallback {
   private static final Log log = LogFactory.getLog(QueueAsyncInvocationStage.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationContext ctx;
   private final VisitableCommand command;

   public QueueAsyncInvocationStage(InvocationContext ctx, VisitableCommand command,
                                    CompletableFuture<?> valueFuture, InvocationCallback function) {
      super(new CompletableFuture<>());
      this.ctx = ctx;
      this.command = command;

      queueAdd(function);
      valueFuture.whenComplete(this);
   }

   @Override
   public Object addCallback(InvocationContext ctx, VisitableCommand command, InvocationCallback function) {
      if (ctx != this.ctx || command != this.command) {
         return new SimpleAsyncInvocationStage(future).addCallback(ctx, command, function);
      }

      if (queueAdd(function)) {
         return this;
      }

      return invokeDirectly(ctx, command, function);
   }

   private Object invokeDirectly(InvocationContext ctx, VisitableCommand command, InvocationCallback function) {
      Object rv;
      Throwable throwable;
      // Leave the context temporarily
      ctx.exit();
      try {
         rv = future.join();
         throwable = null;
      } catch (Throwable t) {
         rv = null;
         throwable = CompletableFutures.extractException(t);
      } finally {
         CompletionStage<Void> cs = ctx.enter();
         if (cs != null) {
            if (trace) {
               log.trace("Blocking before invoking directly");
            }
            cs.toCompletableFuture().join();
         }
      }

      try {
         return function.apply(ctx, command, rv, throwable);
      } catch (Throwable t) {
         return new SimpleAsyncInvocationStage(t);
      }
   }

   @Override
   public void accept(Object rv, Throwable throwable) {
      // We started with a CompletableFuture, which is now complete.
      invokeQueuedHandlers(rv, throwable);
   }

   @Override
   public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv, Throwable throwable)
         throws Throwable {
      // We started from another AsyncInvocationStage, which is now complete.
      invokeQueuedHandlers(rv, throwable);

      if (throwable == null) {
         return rv;
      } else {
         throw throwable;
      }
   }

   private void invokeQueuedHandlers(Object rv, Throwable throwable) {
      CompletionStage<Void> cs = ctx.enter();
      if (cs != null) {
         Object finalRv = rv;
         Throwable finalThrowable = throwable;
         cs.whenComplete((nil, t) -> {
            if (trace && t != null) {
               log.tracef(t, "Other blocking operation completed with exception, continuing with %s/%s", finalRv, finalThrowable);
            }
            invokeQueuedHandlers(finalRv, finalThrowable);
         });
         return;
      }
      if (trace)
         log.tracef("Resuming invocation of command %s with %d handlers", command, queueSize());
      while (true) {
         InvocationCallback function = queuePoll();
         if (function == null) {
            // Complete the future.
            // We finished running the handlers, and the last pollHandler() call locked the queue.
            if (throwable == null) {
               future.complete(rv);
            } else {
               future.completeExceptionally(throwable);
            }
            return;
         }

         // Run the handler
         try {
            if (throwable != null) {
               throwable = CompletableFutures.extractException(throwable);
            }
            rv = function.apply(ctx, command, rv, throwable);
            throwable = null;
         } catch (Throwable t) {
            rv = null;
            throwable = t;
         }
         if (rv instanceof SimpleAsyncInvocationStage) {
            SimpleAsyncInvocationStage currentStage = (SimpleAsyncInvocationStage) rv;
            if (!currentStage.isDone()) {
               if (currentStage instanceof QueueAsyncInvocationStage) {
                  QueueAsyncInvocationStage queueAsyncInvocationStage = (QueueAsyncInvocationStage) currentStage;
                  queueAsyncInvocationStage.future.whenComplete(this);
                  ctx.exit();
                  return;
               } else {
                  // Use the CompletableFuture directly, without creating another AsyncInvocationStage instance
                  currentStage.future.whenComplete(this);
                  ctx.exit();
                  return;
               }
            } else {
               try {
                  rv = currentStage.get();
               } catch (Throwable t) {
                  throwable = t;
               }
            }
         }
         // We got a synchronous invocation stage, continue with the next handler
      }
   }


   /**
    * An inline implementation of a queue. The queue is frozen automatically when the last element is polled.
    */
   // Capacity must be a power of 2
   static final int QUEUE_INITIAL_CAPACITY = 8;
   @GuardedBy("this")
   private InvocationCallback[] elements = null;
   @GuardedBy("this")
   private byte mask;
   @GuardedBy("this")
   private byte head;
   @GuardedBy("this")
   private byte tail;

   @GuardedBy("this")
   private boolean frozen;

   boolean queueAdd(InvocationCallback element) {
      synchronized (this) {
         if (frozen) {
            return false;
         }
         if (elements == null || tail - head > mask) {
            queueExpand();
         }
         elements[tail & mask] = element;
         tail++;
         return true;
      }
   }

   /**
    * Remove one handler from the deque, or freeze the deque if there are no more elements.
    *
    * @return The next handler, or {@code null} if the deque is empty.
    */
   InvocationCallback queuePoll() {
      InvocationCallback element;
      synchronized (this) {
         if (tail != head) {
            element = elements[head & mask];
            head++;
         } else {
            element = null;
            frozen = true;
         }
      }
      return element;
   }

   /**
    * @return The current number of elements in the deque, only useful for debugging.
    */
   int queueSize() {
      synchronized (this) {
         // We can assume tail and head won't overflow in our use case
         return tail - head;
      }
   }

   @GuardedBy("this")
   private void queueExpand() {
      // We start with no elements and mask 0
      if (elements == null) {
         elements = new InvocationCallback[QUEUE_INITIAL_CAPACITY];
         mask = QUEUE_INITIAL_CAPACITY - 1;
         return;
      }

      InvocationCallback[] oldElements = elements;
      int oldCapacity = oldElements.length;
      int oldMask = mask;
      int oldHead = head;
      int oldTail = tail;
      int maskedHead = oldHead & oldMask;
      int maskedTail = oldTail & oldMask;
      int oldSize = tail - head;
      if (oldSize != oldCapacity)
         throw new IllegalStateException("Queue should be expanded only when full");

      int newSize = oldCapacity * 2;
      int newMask = newSize - 1;
      elements = new InvocationCallback[newSize];
      mask = (byte) newMask;
      head = 0;
      tail = (byte) (oldTail - oldHead);

      if (maskedHead < maskedTail) {
         System.arraycopy(oldElements, maskedHead, elements, 0, oldSize);
      } else {
         System.arraycopy(oldElements, maskedHead, elements, 0, oldCapacity - maskedHead);
         System.arraycopy(oldElements, 0, elements, oldCapacity - maskedHead, maskedTail);
      }
   }

   @Override
   public String toString() {
      return "SimpleAsyncInvocationStage(" + queueSize() + "handlers, "+ future + ')';
   }
}
