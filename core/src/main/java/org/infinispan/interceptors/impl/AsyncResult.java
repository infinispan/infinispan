package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;

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
class AsyncResult extends CompletableFuture<Object> {
   interface DequeInvoker {
      // Just a pretty name, no methods
   }

   interface Invoker0 extends DequeInvoker {
      Object invoke(Object callback, Object returnValue, Throwable throwable);
   }

   interface Invoker1 extends DequeInvoker {
      Object invoke(Object callback, Object p1, Object returnValue, Throwable throwable);
   }

   interface Invoker2 extends DequeInvoker {
      Object invoke(Object callback, Object p1, Object p2, Object returnValue, Throwable throwable);
   }

   static AsyncResult makeExceptional(Throwable throwable) {
      AsyncResult result = new AsyncResult();
      result.dequeFreeze();
      result.completeExceptionally(throwable);
      return result;
   }

   AsyncResult() {
   }

   void complete(Object o, Throwable t) {
      if (t == null) {
         complete(o);
      } else {
         completeExceptionally(t);
      }
   }

   /**
    * An inline implementation of a deque with a fixed size that can be frozen so that no more elements
    * can be added.
    *
    * The deque is frozen manually, with {@link #dequeFreeze()}, or automatically when the last handler is polled,
    * or when the deque reaches capacity.
    */
   // Capacity must be a power of 2
   private static final int DEQUE_INITIAL_CAPACITY = 16;
   @GuardedBy("this")
   private Object[] elements;
   @GuardedBy("this")
   private byte mask;
   @GuardedBy("this")
   private byte head;
   @GuardedBy("this")
   private byte tail;
   @GuardedBy("this")
   private boolean frozen;

   /**
    * Stop adding new elements to this stage
    */
   void dequeFreeze() {
      synchronized (this) {
         frozen = true;
      }
   }

   /**
    * Add a new handler to the deque.
    *
    * @return {@code true} if the handler was added, or {@code false} if the elements deque is frozen
    */
   boolean dequeAdd0(Invoker0 invoker, Object handler) {
      synchronized (this) {
         if (!dequeBasicAdd(invoker, handler, 2))
            return false;

         return true;
      }
   }

   /**
    * Add a new handler to the deque, with one external parameter ({@code p1}).
    *
    * @return {@code true} if the handler was added, or {@code false} if the elements deque is frozen
    */
   boolean dequeAdd1(Invoker1 invoker, Object handler, Object p1) {
      synchronized (this) {
         if (!dequeBasicAdd(invoker, handler, 3))
            return false;

         elements[tail++ & mask] = p1;
         return true;
      }
   }

   /**
    * Add a new handler to the deque, with two external parameters ({@code p1} and {@code p1}).
    *
    * @return {@code true} if the handler was added, or {@code false} if the elements deque is frozen
    */
   boolean dequeAdd2(Invoker2 invoker, Object handler, Object p1, Object p2) {
      synchronized (this) {
         if (!dequeBasicAdd(invoker, handler, 4))
            return false;

         elements[tail++ & mask] = p1;
         elements[tail++ & mask] = p2;
         return true;
      }
   }

   @GuardedBy("this")
   private boolean dequeBasicAdd(DequeInvoker invoker, Object handler, int addCount) {
      if (frozen) {
         return false;
      }
      if (tail - head + addCount > mask) {
         dequeExpand();
      }
      elements[tail++ & mask] = invoker;
      elements[tail++ & mask] = handler;
      return true;
   }

   /**
    * Remove one handler from the deque, or freeze the deque if there are no more elements.
    *
    * @return The next handler, or {@code null} if the deque is empty.
    */
   <T> T dequePoll() {
      Object element;
      synchronized (this) {
         if (tail != head) {
            element = elements[head & mask];
            head++;
         } else {
            element = null;
            dequeFreeze();
         }
      }
      return (T) element;
   }

   /**
    * @return The current number of elements in the deque, only useful for debugging.
    */
   int dequeSize() {
      synchronized (this) {
         // We can assume tail and head won't overflow
         return tail - head;
      }
   }

   @GuardedBy("this")
   private void dequeInit() {
      elements = new Object[DEQUE_INITIAL_CAPACITY];
      mask = DEQUE_INITIAL_CAPACITY - 1;
   }

   @GuardedBy("this")
   private void dequeExpand() {
      Object[] oldHandlers = elements;
      int oldMask = mask;
      int oldSize = oldMask + 1;

      int newSize = oldSize * 2;
      int newMask = newSize - 1;
      elements = new Object[newSize];
      mask = (byte) newMask;

      int maskedHead = head & mask;
      System.arraycopy(oldHandlers, maskedHead, elements, 0, oldSize - maskedHead);
      System.arraycopy(oldHandlers, 0, elements, maskedHead, maskedHead);
   }
}
