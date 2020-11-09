package org.infinispan.persistence.sifs;

import io.reactivex.rxjava3.internal.fuseable.SimpleQueue;
import io.reactivex.rxjava3.internal.queue.MpscLinkedQueue;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Multiple producer-single consumer queue. The producers are expected to call pushAndWait(),
 * the consumer should call pop() in a loop and if a null is returned (meaning that the queue is either empty, to call notifyAndWait().
 *
 * Example:
 * while (running) {
 *    T element = queue.pop();
 *    if (element != null) {
 *       process(element);
 *    } else {
 *       flush();
 *       queue.notifyAndWait();
 *    }
 * }
 * // terminate producers and process the rest of the queue
 * queue.notifyNoWait();
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @author Francesco Nigro &lt;fnigro@redhat.com&gt;
 */
public class SyncProcessingQueue<T> {

   private static final int BUSY_SPIN = Runtime.getRuntime().availableProcessors() == 1? 0 : 10;
   /**
    * This value is based on a raw estimate on Thread::yield cost, but could have been turned into a timed
    * Thread::onSpinWait loop as SynchronousQueue does - see SynchronousQueue.SPIN_FOR_TIMEOUT_THRESHOLD ie 1 us
    */
   private static final int YIELD_SPIN = 12;

   private static final Object COMPLETED = new Object();
   private static final Object ERROR = new Object();

   private final SimpleQueue<AtomicReference<Object>> queue;
   private final ArrayDeque<AtomicReference<Object>> popped;
   private volatile boolean error;
   private volatile Thread blockedConsumer;

   public SyncProcessingQueue() {
      error = false;
      queue = new MpscLinkedQueue<>();
      popped = new ArrayDeque<>();
      blockedConsumer = null;
   }

   public void pushAndWait(T element) throws InterruptedException {
      if (error) {
         throw new IllegalStateException("Exception in consumer");
      }
      // items could be reused, but need to take care of the completion conditions!
      final AtomicReference<Object> holder = new AtomicReference<>();
      holder.lazySet(element);
      queue.offer(holder);
      final Thread blockedConsumer = this.blockedConsumer;
      if (blockedConsumer != null) {
         LockSupport.unpark(blockedConsumer);
      }
      try {
         for (int i = 0; i < BUSY_SPIN; i++) {
            final Object e = holder.get();
            if (e == null) {
               return;
            }
            if (e == ERROR) {
               throw new IllegalStateException("Exception in consumer");
            }
         }
         for (int i = 0; i < YIELD_SPIN; i++) {
            final Object e = holder.get();
            if (e == null) {
               return;
            }
            if (e == ERROR) {
               throw new IllegalStateException("Exception in consumer");
            }
            Thread.yield();
         }
         synchronized (holder) {
            Object e;
            while ((e = holder.get()) != null) {
               if (e == ERROR) {
                  throw new IllegalStateException("Exception in consumer");
               }
               holder.wait();
            }
         }
      } finally {
         // the consumer slow path won't wait completion to happen!
         holder.lazySet(COMPLETED);
      }
   }

   public T pop() {
      return pop(false);
   }

   private T pop(boolean withError) {
      assert error == withError;
      AtomicReference<Object> holder = null;
      try {
         holder = queue.poll();
      } catch (Throwable throwable) {
         // IMPOSSIBLE, damn RxJava APIs!
         assert false;
      }
      if (holder == null) {
         return null;
      }
      final T e = (T) holder.get();
      assert e != null && e != COMPLETED && e != ERROR;
      if (withError) {
         handleItem(holder, true);
      } else {
         final boolean alwaysTrue = popped.offer(holder);
         assert alwaysTrue;
      }
      return e;
   }

   private void handleItem(AtomicReference<Object> holder, boolean withError) {
      holder.lazySet(withError? ERROR : null);
      // try save notifying producers
      for (int i = 0; i < BUSY_SPIN; i++) {
         if (holder.get() == COMPLETED) {
            return;
         }
      }
      for (int i = 0; i < YIELD_SPIN; i++) {
         if (holder.get() == COMPLETED) {
            return;
         }
         Thread.yield();
      }
      if (holder.get() == COMPLETED) {
         return;
      }
      synchronized (holder) {
         holder.notify();
      }
   }

   public void notifyNoWait() {
      assert !error;
      AtomicReference<Object> holder;
      while ((holder = popped.poll()) != null) {
         handleItem(holder, false);
      }
   }

   /**
    * @return {code true} if not empty or {@code false} if the consumer Thread is interrupted
    */
   public boolean notifyAndWait() {
      assert !error;
      AtomicReference<Object> holder;
      while ((holder = popped.poll()) != null) {
         handleItem(holder, false);
      }
      final Thread currentThread = Thread.currentThread();
      while (queue.isEmpty()) {
         blockedConsumer = currentThread;
         // StoreLoad here: some offers could have slipped in before perceiving
         // any blockedConsumer, must check it before going to sleep for real
         // or we risk to be parked forever
         try {
            if (!queue.isEmpty()) {
               return true;
            }
            LockSupport.park();
            if (currentThread.isInterrupted()) {
               return false;
            }
         } finally {
            blockedConsumer = null;
         }
      }
      return true;
   }

   public void notifyError() {
      error = true;
      // first cleanup already popped elements first
      AtomicReference<Object> holder;
      while ((holder = popped.poll()) != null) {
         handleItem(holder, true);
      }
      // cleanup pending ones
      while (pop(true) != null) {
      }
   }
}
