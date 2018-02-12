package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.AbstractIterator;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscArrayQueue;
import io.reactivex.internal.subscriptions.SubscriptionHelper;

/**
 * An iterable that will automatically poll entries from the given publishers. Entries are first attempted to be
 * returned from the first publisher and so forth. If no publisher has recently published an entry the iterator
 * will block until one does so or all are known to have completed.
 * <p>
 * The iterator returned should be closed by the user when they are done to ensure resources are freed properly.
 * @author wburns
 * @since 9.0
 */
public class PriorityMergingProcessor<T> implements CloseableIterable<T> {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Collection<PublisherIntPair<T>> pairs;

   public static <T> PriorityMergingProcessor<T> build(Publisher<T> publisher, int firstbatchSize, Publisher<T> secondPublisher,
         int secondBatchSize) {
      return new PriorityMergingProcessor<>(publisher, firstbatchSize, secondPublisher, secondBatchSize);
   }

   public static <T> Builder<T> builder() {
      return new Builder<>();
   }

   private static class PublisherIntPair<T> {
      private final Publisher<T> publisher;
      private final int batchSize;

      private PublisherIntPair(Publisher<T> publisher, int batchSize) {
         this.publisher = Objects.requireNonNull(publisher);
         if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be greater than 0");
         }
         this.batchSize = batchSize;
      }
   }

   public static class Builder<T> {
      Stream.Builder<PublisherIntPair<T>> current = Stream.builder();

      Builder<T> addPublisher(Publisher<T> publisher, int batchSize) {
         current.accept(new PublisherIntPair<T>(publisher, batchSize));
         return this;
      }

      PriorityMergingProcessor<T> build() {
         return new PriorityMergingProcessor<>(current.build().collect(Collectors.toList()));
      }
   }

   private PriorityMergingProcessor(Publisher<T> publisher, int firstbatchSize, Publisher<T> secondPublisher,
         int secondBatchSize) {
      this.pairs = Arrays.asList(new PublisherIntPair<T>(publisher, firstbatchSize),
            new PublisherIntPair<T>(secondPublisher, secondBatchSize));
   }

   private PriorityMergingProcessor(Collection<PublisherIntPair<T>> pairs) {
      this.pairs = pairs;
   }

   @Override
   public void close() {
      // This does nothing, the iterators need to be closed individually
   }

   @Override
   public CloseableIterator<T> iterator() {
      MultiSubscriberIterator<T> iterator = new MultiSubscriberIterator<>(pairs);
      iterator.start();
      return iterator;
   }


   private static final class MultiSubscriberIterator<T> extends AbstractIterator<T> implements CloseableIterator<T> {

      private final QueueSubscriber<T>[] queueSubscribers;

      private final Lock lock;

      private final Condition condition;

      private boolean signalled;

      volatile Throwable error;

      MultiSubscriberIterator(Collection<PublisherIntPair<T>> pairs) {
         this.queueSubscribers = new QueueSubscriber[pairs.size()];

         this.signalled = false;
         this.lock = new ReentrantLock();
         this.condition = lock.newCondition();

         int offset = 0;
         for (PublisherIntPair<T> pair : pairs) {
            QueueSubscriber<T> actualSubscriber = new QueueSubscriber<>(pair.publisher, pair.batchSize, this);
            queueSubscribers[offset++] = actualSubscriber;
         }
      }

      /**
       * Actually starts each subscriber - needed because otherwise subscriber could call back while in constructor
       */
      public void start() {
         for (QueueSubscriber<T> queueSubscriber : queueSubscribers) {
            queueSubscriber.start();
         }
      }

      static RuntimeException wrapOrThrow(Throwable error) {
         if (!(error instanceof CacheException)) {
            return new CacheException(error);
         }
         return (CacheException) error;
      }

      @Override
      public void close() {
         for (QueueSubscriber<T> queueSubscriber : queueSubscribers) {
            queueSubscriber.close();
         }
         signalConsumer();
      }

      @Override
      protected T getNext() {
         do {
            boolean allDone = true;
            for (QueueSubscriber<T> t : queueSubscribers) {
               T nextValue = t.poll();
               if (nextValue != null) {
                  return nextValue;
               }
               allDone &= t.isDone();
            }
            // We check error afterwards to avoid additional read for vast majority of cases
            if (error != null) {
               throw wrapOrThrow(error);
            }
            if (allDone) {
               return null;
            }
            lock.lock();
            try {
               while (true) {
                  if (signalled) {
                     signalled = false;
                     break;
                  }
                  try {
                     if (!condition.await(30, TimeUnit.SECONDS)) {
                        log.debugf("Timeout encountered: error %s, queues %s", error, Arrays.toString(queueSubscribers));
                        throw new TimeoutException();
                     }
                  } catch (InterruptedException e) {
                     throw wrapOrThrow(e);
                  }
               }
            } finally {
               lock.unlock();
            }
         }
         while (true);
      }

      void onError(Throwable t) {
         error = t;
         signalConsumer();
      }

      void signalConsumer() {
         lock.lock();
         try {
            // Only signal the condition if we had someone fail to take a value
            if (!signalled) {
               signalled = true;
               condition.signalAll();
            }
         } finally {
            lock.unlock();
         }
      }
   }

   static private final class QueueSubscriber<T> extends AtomicReference<Subscription> implements Subscriber<T>, AutoCloseable {
      private final Publisher<T> publisher;
      private final SimplePlainQueue<T> queue;
      private final long batchSize;
      private final long limit;
      private MultiSubscriberIterator notifier;

      private long produced;

      private volatile boolean done;

      QueueSubscriber(Publisher<T> publisher, int batchSize, MultiSubscriberIterator subscriber) {
         this.publisher = publisher;
         this.queue = new SpscArrayQueue<>(batchSize);
         this.batchSize = batchSize;
         this.notifier = subscriber;
         this.limit = batchSize - (batchSize >> 2);
      }

      void start() {
         publisher.subscribe(this);
      }

      /**
       * Returns value if available as well as requesting more from producer as needed.
       */
      public T poll() {
         T returned = queue.poll();
         if (returned != null) {
            long p = produced + 1;
            if (p == limit) {
               produced = 0;
               get().request(p);
            } else {
               produced = p;
            }
         }
         return returned;
      }

      @Override
      public void onSubscribe(Subscription s) {
         if (SubscriptionHelper.setOnce(this, s)) {
            s.request(batchSize);
         }
      }

      @Override
      public void onNext(T t) {
         if (!queue.offer(t)) {
            SubscriptionHelper.cancel(this);

            onError(new IllegalStateException("Too many items requested, this is a bug! - produced=" + produced + ", subscription=" + get()));
         } else {
            // This is just to wake them up - in case if they are waiting for an item
            notifier.signalConsumer();
         }
      }

      @Override
      public void onError(Throwable t) {
         done = true;
         notifier.onError(t);
      }

      @Override
      public void onComplete() {
         done = true;
         // We have to signal consumer, just in case if they were waiting on us to return, but we ended up completing
         // instead.
         notifier.signalConsumer();
      }

      @Override
      public void close() {
         SubscriptionHelper.cancel(this);
      }

      public boolean isDone() {
         return done;
      }

      @Override
      public String toString() {
         return "QueueSubscriber{" +
               "queue.empty = " + queue.isEmpty() + ", " +
               "done = " + done +
               "}";
      }
   }
}
