package org.infinispan.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;

/**
 * Delegate that forwards all the of the method calls to the underlying cache stream. This can be useful to intercept
 * a given method call. Note that primitive stream operations are not intercepted (including those from
 * {@link java.util.stream.BaseStream}, however defined {@link org.infinispan.BaseCacheStream} methods would be.
 */
public class AbstractDelegatingCacheStream<R> implements CacheStream<R> {
   protected CacheStream<?> underlyingStream;

   public AbstractDelegatingCacheStream(CacheStream<R> stream) {
      this.underlyingStream = stream;
   }

   private CacheStream<R> castStream(CacheStream stream) {
      return stream;
   }

   // These are methods that convert to a different AbstractDelegating*CacheStream

   @Override
   public IntCacheStream mapToInt(ToIntFunction<? super R> mapper) {
      return new AbstractDelegatingIntCacheStream(this, castStream(underlyingStream).mapToInt(mapper));
   }

   @Override
   public LongCacheStream mapToLong(ToLongFunction<? super R> mapper) {
      return new AbstractDelegatingLongCacheStream(this, castStream(underlyingStream).mapToLong(mapper));
   }

   @Override
   public DoubleCacheStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      return new AbstractDelegatingDoubleCacheStream(this, castStream(underlyingStream).mapToDouble(mapper));
   }

   @Override
   public IntCacheStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      return new AbstractDelegatingIntCacheStream(this, castStream(underlyingStream).flatMapToInt(mapper));
   }

   @Override
   public LongCacheStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      return new AbstractDelegatingLongCacheStream(this, castStream(underlyingStream).flatMapToLong(mapper));
   }

   @Override
   public DoubleCacheStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      return new AbstractDelegatingDoubleCacheStream(this, castStream(underlyingStream).flatMapToDouble(mapper));
   }

   @Override
   public AbstractDelegatingCacheStream<R> sequentialDistribution() {
      underlyingStream = underlyingStream.sequentialDistribution();
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> parallelDistribution() {
      underlyingStream = underlyingStream.parallelDistribution();
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> filterKeySegments(Set<Integer> segments) {
      underlyingStream = underlyingStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> filterKeys(Set<?> keys) {
      underlyingStream = underlyingStream.filterKeys(keys);
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> distributedBatchSize(int batchSize) {
      underlyingStream = underlyingStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> segmentCompletionListener(SegmentCompletionListener listener) {
      underlyingStream = underlyingStream.segmentCompletionListener(listener);
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> disableRehashAware() {
      underlyingStream = underlyingStream.disableRehashAware();
      return this;
   }

   @Override
   public AbstractDelegatingCacheStream<R> timeout(long timeout, TimeUnit unit) {
      underlyingStream = underlyingStream.timeout(timeout, unit);
      return this;
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      castStream(underlyingStream).forEach(action);
   }

   @Override
   public <K, V> void forEach(BiConsumer<Cache<K, V>, ? super R> action) {
      castStream(underlyingStream).forEach(action);
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      castStream(underlyingStream).forEachOrdered(action);
   }

   @Override
   public Object[] toArray() {
      return underlyingStream.toArray();
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return underlyingStream.toArray(generator);
   }

   @Override
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return castStream(underlyingStream).reduce(identity, accumulator);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      return castStream(underlyingStream).reduce(accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return castStream(underlyingStream).reduce(identity, accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return castStream(underlyingStream).collect(supplier, accumulator, combiner);
   }

   @Override
   public Iterator<R> iterator() {
      return castStream(underlyingStream).iterator();
   }

   @Override
   public Spliterator<R> spliterator() {
      return castStream(underlyingStream).spliterator();
   }

   @Override
   public boolean isParallel() {
      return underlyingStream.isParallel();
   }

   @Override
   public CacheStream<R> sequential() {
      underlyingStream = underlyingStream.sequential();
      return this;
   }

   @Override
   public CacheStream<R> parallel() {
      underlyingStream = underlyingStream.parallel();
      return this;
   }

   @Override
   public CacheStream<R> unordered() {
      underlyingStream = underlyingStream.unordered();
      return this;
   }

   @Override
   public CacheStream<R> onClose(Runnable closeHandler) {
      underlyingStream = underlyingStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      underlyingStream.close();
   }

   @Override
   public CacheStream<R> sorted() {
      underlyingStream = underlyingStream.sorted();
      return this;
   }

   @Override
   public CacheStream<R> sorted(Comparator<? super R> comparator) {
      underlyingStream = castStream(underlyingStream).sorted(comparator);
      return this;
   }

   @Override
   public CacheStream<R> peek(Consumer<? super R> action) {
      underlyingStream = castStream(underlyingStream).peek(action);
      return this;
   }

   @Override
   public CacheStream<R> limit(long maxSize) {
      underlyingStream = underlyingStream.limit(maxSize);
      return this;
   }

   @Override
   public CacheStream<R> skip(long n) {
      underlyingStream = underlyingStream.skip(n);
      return this;
   }

   @Override
   public CacheStream<R> filter(Predicate<? super R> predicate) {
      underlyingStream = castStream(underlyingStream).filter(predicate);
      return this;
   }

   @Override
   public <R1> CacheStream<R1> map(Function<? super R, ? extends R1> mapper) {
      underlyingStream = castStream(underlyingStream).map(mapper);
      return (CacheStream<R1>) this;
   }

   @Override
   public <R1> CacheStream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      underlyingStream = castStream(underlyingStream).flatMap(mapper);
      return (CacheStream<R1>) this;
   }

   @Override
   public CacheStream<R> distinct() {
      underlyingStream = underlyingStream.distinct();
      return this;
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      return castStream(underlyingStream).collect(collector);
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      return castStream(underlyingStream).min(comparator);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      return castStream(underlyingStream).max(comparator);
   }

   @Override
   public long count() {
      return underlyingStream.count();
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return castStream(underlyingStream).anyMatch(predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return castStream(underlyingStream).allMatch(predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return castStream(underlyingStream).noneMatch(predicate);
   }

   @Override
   public Optional<R> findFirst() {
      return castStream(underlyingStream).findFirst();
   }

   @Override
   public Optional<R> findAny() {
      return castStream(underlyingStream).findAny();
   }
}
