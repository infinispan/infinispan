package org.infinispan.util;

import java.util.IntSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.IntBinaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;

/**
 * Abstract Delegating handler that passes IntStream operations off to the underlying IntCacheStream but delegates
 * {@link org.infinispan.BaseCacheStream} operations to the provided {@link CacheStream}. This allows for intercepting
 * methods defined on <b>BaseCacheStream</b>.
 * <p>
 * This class is package private as it should only be created by using a map operator from another
 * AbstractDelegating*CacheStream instance. Note that {@link AbstractDelegatingCacheStream} is public as this is
 * the defined approach to create such a delegated stream.
 * @author wburns
 * @since 9.2
 */
class AbstractDelegatingIntCacheStream implements IntCacheStream {
   protected AbstractDelegatingCacheStream<?> delegateCacheStream;
   protected IntCacheStream underlyingStream;

   AbstractDelegatingIntCacheStream(AbstractDelegatingCacheStream<?> delegateCacheStream,
         IntCacheStream underlyingStream) {
      this.delegateCacheStream = delegateCacheStream;
      this.underlyingStream = underlyingStream;
   }

   // These are methods that convert to a different AbstractDelegating*CacheStream

   @Override
   public DoubleCacheStream mapToDouble(IntToDoubleFunction mapper) {
      return new AbstractDelegatingDoubleCacheStream(delegateCacheStream, underlyingStream.mapToDouble(mapper));
   }

   @Override
   public LongCacheStream mapToLong(IntToLongFunction mapper) {
      return new AbstractDelegatingLongCacheStream(delegateCacheStream, underlyingStream.mapToLong(mapper));
   }

   @Override
   public <U> CacheStream<U> mapToObj(IntFunction<? extends U> mapper) {
      delegateCacheStream.underlyingStream = underlyingStream.mapToObj(mapper);
      return (CacheStream<U>) delegateCacheStream;
   }

   @Override
   public CacheStream<Integer> boxed() {
      delegateCacheStream.underlyingStream = underlyingStream.boxed();
      return (CacheStream<Integer>) delegateCacheStream;
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return new AbstractDelegatingDoubleCacheStream(delegateCacheStream, underlyingStream.asDoubleStream());
   }

   @Override
   public LongCacheStream asLongStream() {
      return new AbstractDelegatingLongCacheStream(delegateCacheStream, underlyingStream.asLongStream());
   }

   // These are methods that should delegate to the original cache stream

   @Override
   public IntCacheStream sequentialDistribution() {
      delegateCacheStream = delegateCacheStream.sequentialDistribution();
      return this;
   }

   @Override
   public IntCacheStream parallelDistribution() {
      delegateCacheStream = delegateCacheStream.parallelDistribution();
      return this;
   }

   @Override
   public IntCacheStream filterKeySegments(Set<Integer> segments) {
      delegateCacheStream = delegateCacheStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public IntCacheStream filterKeys(Set<?> keys) {
      delegateCacheStream = delegateCacheStream.filterKeys(keys);
      return this;
   }

   @Override
   public IntCacheStream distributedBatchSize(int batchSize) {
      delegateCacheStream = delegateCacheStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public IntCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      delegateCacheStream = delegateCacheStream.segmentCompletionListener(listener);
      return this;
   }

   @Override
   public IntCacheStream disableRehashAware() {
      delegateCacheStream = delegateCacheStream.disableRehashAware();
      return this;
   }

   @Override
   public IntCacheStream timeout(long timeout, TimeUnit unit) {
      delegateCacheStream = delegateCacheStream.timeout(timeout, unit);
      return this;
   }

   // Actual IntStream operations

   @Override
   public IntCacheStream filter(IntPredicate predicate) {
      underlyingStream = underlyingStream.filter(predicate);
      return this;
   }

   @Override
   public IntCacheStream map(IntUnaryOperator mapper) {
      underlyingStream = underlyingStream.map(mapper);
      return this;
   }

   @Override
   public IntCacheStream flatMap(IntFunction<? extends IntStream> mapper) {
      underlyingStream = underlyingStream.flatMap(mapper);
      return this;
   }

   @Override
   public IntCacheStream distinct() {
      underlyingStream = underlyingStream.distinct();
      return this;
   }

   @Override
   public IntCacheStream sorted() {
      underlyingStream = underlyingStream.sorted();
      return this;
   }

   @Override
   public IntCacheStream peek(IntConsumer action) {
      underlyingStream = underlyingStream.peek(action);
      return this;
   }

   @Override
   public IntCacheStream limit(long maxSize) {
      underlyingStream = underlyingStream.limit(maxSize);
      return this;
   }

   @Override
   public IntCacheStream skip(long n) {
      underlyingStream = underlyingStream.skip(n);
      return this;
   }

   @Override
   public void forEach(IntConsumer action) {
      underlyingStream.forEach(action);
   }

   @Override
   public void forEachOrdered(IntConsumer action) {
      underlyingStream.forEachOrdered(action);
   }

   @Override
   public int[] toArray() {
      return underlyingStream.toArray();
   }

   @Override
   public int reduce(int identity, IntBinaryOperator op) {
      return underlyingStream.reduce(identity, op);
   }

   @Override
   public OptionalInt reduce(IntBinaryOperator op) {
      return underlyingStream.reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return underlyingStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public int sum() {
      return underlyingStream.sum();
   }

   @Override
   public OptionalInt min() {
      return underlyingStream.min();
   }

   @Override
   public OptionalInt max() {
      return underlyingStream.max();
   }

   @Override
   public long count() {
      return underlyingStream.count();
   }

   @Override
   public OptionalDouble average() {
      return underlyingStream.average();
   }

   @Override
   public IntSummaryStatistics summaryStatistics() {
      return underlyingStream.summaryStatistics();
   }

   @Override
   public boolean anyMatch(IntPredicate predicate) {
      return underlyingStream.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return underlyingStream.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return underlyingStream.noneMatch(predicate);
   }

   @Override
   public OptionalInt findFirst() {
      return underlyingStream.findFirst();
   }

   @Override
   public OptionalInt findAny() {
      return underlyingStream.findAny();
   }

   @Override
   public <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action) {
      underlyingStream.forEach(action);
   }

   @Override
   public IntCacheStream sequential() {
      underlyingStream = underlyingStream.sequential();
      return this;
   }

   @Override
   public IntCacheStream parallel() {
      underlyingStream = underlyingStream.parallel();
      return this;
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return underlyingStream.iterator();
   }

   @Override
   public Spliterator.OfInt spliterator() {
      return underlyingStream.spliterator();
   }

   @Override
   public boolean isParallel() {
      return underlyingStream.isParallel();
   }

   @Override
   public IntCacheStream unordered() {
      underlyingStream = underlyingStream.unordered();
      return this;
   }

   @Override
   public IntCacheStream onClose(Runnable closeHandler) {
      underlyingStream = underlyingStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      underlyingStream.close();
   }
}
