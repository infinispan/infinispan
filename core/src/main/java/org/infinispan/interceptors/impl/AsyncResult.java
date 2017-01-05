package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.jgroups.annotations.GuardedBy;

/**
 * Invocation stage representing a computation that may or may not be done yet.
 *
 * <p>It stores handler objects in a queue instead of creating a new instance every time a handler is added.
 * It will freeze the queue and stop adding objects to it once the last handler was executed (before calling
 * {@link #complete(Object)}).
 * </p>
 *
 * @author Dan Berindei
 * @since 9.0
 */
class AsyncResult extends CompletableFuture<Object> {
   interface QueueInvoker {
      // Just a pretty name, no methods
   }

   interface Invoker0 extends QueueInvoker {
      Object invoke(Object callback, Object returnValue, Throwable throwable);
   }

   interface Invoker1 extends QueueInvoker {
      Object invoke(Object callback, Object p1, Object returnValue, Throwable throwable);
   }

   interface Invoker2 extends QueueInvoker {
      Object invoke(Object callback, Object p1, Object p2, Object returnValue, Throwable throwable);
   }

   static AsyncResult makeExceptional(Throwable throwable) {
      AsyncResult result = new AsyncResult();
      result.queueFreeze();
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
    * The deque is frozen manually, with {@link #queueFreeze()}, or automatically when the last handler is polled,
    * or when the deque reaches capacity.
    */
   // Capacity must be a power of 2
   static final int QUEUE_INITIAL_CAPACITY = 16;
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
    * Add a new handler to the deque.
    *
    * @return {@code true} if the handler was added, or {@code false} if the elements deque is frozen
    */
   boolean queueAdd(Invoker0 invoker, Object handler) {
      synchronized (this) {
         return queueBasicAdd(invoker, handler, 2);
      }
   }

   /**
    * Add a new handler to the deque, with one external parameter ({@code p1}).
    *
    * @return {@code true} if the handler was added, or {@code false} if the elements deque is frozen
    */
   boolean queueAdd(Invoker1 invoker, Object handler, Object p1) {
      synchronized (this) {
         if (!queueBasicAdd(invoker, handler, 3))
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
   boolean queueAdd(Invoker2 invoker, Object handler, Object p1, Object p2) {
      synchronized (this) {
         if (!queueBasicAdd(invoker, handler, 4))
            return false;

         elements[tail++ & mask] = p1;
         elements[tail++ & mask] = p2;
         return true;
      }
   }

   @GuardedBy("this")
   private boolean queueBasicAdd(QueueInvoker invoker, Object handler, int addCount) {
      if (frozen) {
         return false;
      }
      // Assume no overflow on the head/tail
      if (tail - head + addCount > mask) {
         queueExpand();
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
   <T> T queuePoll() {
      Object element;
      synchronized (this) {
         if (tail != head) {
            element = elements[head & mask];
            head++;
         } else {
            element = null;
            queueFreeze();
         }
      }
      return (T) element;
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

   Object queueFindFirst(Predicate<Object> predicate) {
      synchronized (this) {
         // We can assume tail and head won't overflow in our use case
         for (int i = head; i < tail; i++) {
            Object element = elements[i & mask];
            if (predicate.test(element)) {
               return element;
            }
         }
      }
      return null;
   }

   /**
    * Stop adding new elements to this stage
    */
   private void queueFreeze() {
      synchronized (this) {
         frozen = true;
      }
   }

   @GuardedBy("this")
   private void queueExpand() {
      // We start with no elements and mask 0
      if (elements == null) {
         elements = new Object[QUEUE_INITIAL_CAPACITY];
         mask = QUEUE_INITIAL_CAPACITY - 1;
         return;
      }

      Object[] oldElements = elements;
      int oldCapacity = oldElements.length;
      int oldMask = mask;
      int oldHead = head;
      int oldTail = tail;
      int maskedHead = oldHead & oldMask;
      int maskedTail = oldTail & oldMask;
      int oldSize = tail - head;

      int newSize = oldCapacity * 2;
      int newMask = newSize - 1;
      elements = new Object[newSize];
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
}
