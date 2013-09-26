package org.infinispan.persistence.sifs;

import java.util.ArrayDeque;

/**
 * Multiple producer-single consumer queue. The producers are expected to call pushAndWait(),
 * the consumer should call pop() in a loop and if a null is returned (meaning that the queue is either empty
 * or the limit of elements processed in a loop has been reached, to call notifyAndWait().
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
 */
public class SyncProcessingQueue<T> {
   private final ArrayDeque<T> queue = new ArrayDeque<T>();
   private final int maxPoppedInRow;
   private final Object sync = new Object();

   private volatile long popIndex = 0;
   private long pushIndex = 0;

   private long processorPopIndex = 0;
   private int poppedInRow = 0;
   private volatile boolean error;

   public SyncProcessingQueue() {
      this(Integer.MAX_VALUE);
   }

   public SyncProcessingQueue(int maxPoppedInRow) {
      this.maxPoppedInRow = maxPoppedInRow;
   }

   public void pushAndWait(T element) throws InterruptedException {
      waitFor(push(element));
   }

   public long push(T element) {
      synchronized (queue) {
         queue.push(element);
         queue.notify();
         pushIndex++;
         return pushIndex;
      }
   }

   protected void waitFor(long myIndex) throws InterruptedException {
      synchronized (sync) {
         while (myIndex > popIndex) {
            sync.wait();
            //Thread.yield();
         }
      }
      if (error) {
         throw new IllegalStateException("Exception in consumer");
      }
   }

   public T pop() {
      if (poppedInRow >= maxPoppedInRow) {
         return null;
      }
      T element;
      synchronized (queue) {
         element = queue.poll();
      }
      if (element == null) {
         return null;
      } else {
         processorPopIndex++;
         poppedInRow++;
         return element;
      }
   }

   public void notifyAndWait() {
      poppedInRow = 0;
      popIndex = processorPopIndex;
      synchronized (sync) {
         sync.notifyAll();
      }
      synchronized (queue) {
         if (queue.isEmpty()) {
            try {
               queue.wait();
            } catch (InterruptedException e) {
               return;
            }
         }
      }
   }

   public void notifyNoWait() {
      poppedInRow = 0;
      popIndex = processorPopIndex;
      synchronized (sync) {
         sync.notifyAll();
      }
   }

   public void notifyError() {
      error = true;
      popIndex = Long.MAX_VALUE;
      synchronized (sync) {
         sync.notifyAll();
      }
   }
}
