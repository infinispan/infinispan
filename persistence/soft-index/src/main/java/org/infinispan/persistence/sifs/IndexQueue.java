package org.infinispan.persistence.sifs;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.equivalence.Equivalence;

/**
 * Splits the requests into several subqueues according to request.key.hashCode(). If the request has no key,
 * inserts countdown into the request and puts it into all subqueues - the thread that retrieves such element
 * should call countDown() and upon true handle the request (this preserves the FIFO ordering).
 *
 * @author Radim Vansa &lt;rvansa@redhat.coPausem&gt;
 */
public class IndexQueue extends AbstractQueue<IndexRequest> implements BlockingQueue<IndexRequest> {

   private final ArrayBlockingQueue[] queues;
   private final Equivalence<Object> keyEquivalence;

   public IndexQueue(int segments, int capacity, Equivalence<Object> keyEquivalence) {
      queues = new ArrayBlockingQueue[segments];
      for (int i = 0; i < segments; ++i) {
         queues[i] = new ArrayBlockingQueue(capacity);
      }
      this.keyEquivalence = keyEquivalence;
   }

   @Override
   public Iterator<IndexRequest> iterator() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int size() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void put(IndexRequest indexRequest) throws InterruptedException {
      if (indexRequest.getKey() != null) {
         queues[Math.abs(keyEquivalence.hashCode(indexRequest.getKey())) % queues.length].put(indexRequest);
      } else {
         indexRequest.setCountDown(queues.length);
         for (int i = 0; i < queues.length; ++i) {
            queues[i].put(indexRequest);
         }
      }
   }

   @Override
   public boolean offer(IndexRequest indexRequest, long timeout, TimeUnit unit) throws InterruptedException {
      if (indexRequest.getKey() != null) {
         return queues[Math.abs(keyEquivalence.hashCode(indexRequest.getKey())) % queues.length].offer(indexRequest, timeout, unit);
      } else {
         throw new UnsupportedOperationException();
      }
   }

   @Override
   public IndexRequest take() throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public IndexRequest poll(long timeout, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public int remainingCapacity() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int drainTo(Collection<? super IndexRequest> c) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int drainTo(Collection<? super IndexRequest> c, int maxElements) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean offer(IndexRequest indexRequest) {
      return queues[Math.abs(keyEquivalence.hashCode(indexRequest.getKey())) % queues.length].offer(indexRequest);
   }

   @Override
   public IndexRequest poll() {
      throw new UnsupportedOperationException();
   }

   @Override
   public IndexRequest peek() {
      throw new UnsupportedOperationException();
   }

   public BlockingQueue<IndexRequest> subQueue(int id) {
      return queues[id];
   }
}
