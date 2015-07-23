package org.infinispan.stream.impl.spliterators;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A Spliterator using the provided iterator for supplying values.
 * Splits occur start at the batch size.  Each split gets subsequently bigger by increasing by the original
 * split size.  The batch size will never become higher than the configured max batch size
 */
public class IteratorAsSpliterator<T> implements CloseableSpliterator<T> {
   private CloseableIterator<? extends T> iterator;
   private final int characteristics;
   private final int batchIncrease;
   private final int maxBatchSize;

   private long estimateRemaining;
   private int currentBatchSize;

   public static class Builder<T> implements Supplier<IteratorAsSpliterator<T>> {
      private final CloseableIterator<? extends T> iterator;
      private int characteristics;
      private int batchIncrease = 1024;
      private int maxBatchSize = 51200;

      private long estimateRemaining = Long.MAX_VALUE;

      public Builder(Iterator<? extends T> iterator) {
         Objects.nonNull(iterator);
         this.iterator = Closeables.iterator(iterator);
      }

      public Builder(CloseableIterator<? extends T> closeableIterator) {
         Objects.nonNull(closeableIterator);
         this.iterator = closeableIterator;
      }

      /**
       * Sets the characteristics the subsequent spliterator will have.
       * @param characteristics
       * @return
       */
      public Builder setCharacteristics(int characteristics) {
         this.characteristics = characteristics;
         return this;
      }

      /**
       * Sets the batch increase size.  This controls how much larger subsequent splits are.
       * The default value is 1024;
       * @param batchIncrease
       * @return this
       */
      public Builder setBatchIncrease(int batchIncrease) {
         if (batchIncrease <= 0) {
            throw new IllegalArgumentException("The batchIncrease " + batchIncrease + " must be greater than 0");
         }
         this.batchIncrease = batchIncrease;
         return this;
      }

      /**
       * Sets the max batch size for a thread to use - This defaults to 51200
       * @param maxBatchSize
       * @return this
       */
      public Builder setMaxBatchSize(int maxBatchSize) {
         if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("The maxBatchSize " + maxBatchSize + " must be greater than 0");
         }
         this.maxBatchSize = maxBatchSize;
         return this;
      }

      /**
       * Sets how many estimated elements are remaining for this iterator
       * This defaults to Long.MAX_VALUE.  It is heavily recommended to provide an exact or estimate value
       * to help with controlling parallelism
       * @param estimateRemaining
       * @return this
       */
      public Builder setEstimateRemaining(long estimateRemaining) {
         this.estimateRemaining = estimateRemaining;
         return this;
      }

      @Override
      public IteratorAsSpliterator<T> get() {
         if (iterator == null) {
            throw new IllegalArgumentException("Iterator cannot be null");
         }
         if (batchIncrease > maxBatchSize) {
            throw new IllegalArgumentException("Max batch size " + maxBatchSize +
                    " cannot be larger than batchIncrease" + batchIncrease);
         }
         return new IteratorAsSpliterator<>(this);
      }
   }

   /**
    *
    * @param builder
    */
   private IteratorAsSpliterator(Builder<T> builder) {
      this.iterator = builder.iterator;
      this.characteristics = builder.characteristics;
      this.batchIncrease = builder.batchIncrease;
      this.maxBatchSize = builder.maxBatchSize;

      this.estimateRemaining = builder.estimateRemaining;
   }

   @Override
   public Spliterator<T> trySplit() {
      if (estimateRemaining > 1 && iterator.hasNext()) {
         int batch = currentBatchSize + batchIncrease;
         if (batch > estimateRemaining) {
            batch = (int) estimateRemaining;
         }
         if (batch > maxBatchSize) {
            batch = maxBatchSize;
         }
         Object[] array = new Object[batch];
         int i = 0;
         while (iterator.hasNext() && i < batch) {
            array[i] = iterator.next();
            i++;
         }
         currentBatchSize = batch;
         estimateRemaining -= i;
         return Spliterators.spliterator(array, 0, i, characteristics);
      }
      return null;
   }

   @Override
   public void forEachRemaining(Consumer<? super T> action) {
      if (action == null) {
         throw new NullPointerException();
      }
      iterator.forEachRemaining(action);
   }

   @Override
   public boolean tryAdvance(Consumer<? super T> action) {
      if (action == null) {
         throw new NullPointerException();
      }
      if (iterator.hasNext()) {
         action.accept(iterator.next());
         return true;
      }
      return false;
   }

   @Override
   public long estimateSize() {
      return estimateRemaining;
   }

   @Override
   public int characteristics() { return characteristics; }

   @Override
   public Comparator<? super T> getComparator() {
      if (hasCharacteristics(Spliterator.SORTED)) {
         return null;
      }
      throw new IllegalStateException();
   }

   @Override
   public void close() {
      iterator.close();
   }
}
