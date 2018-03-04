package org.infinispan.util;

import java.util.LongSummaryStatistics;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongToDoubleFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;

/**
 * Abstract Delegating handler that passes LongStream operations off to the underlying LongCacheStream but delegates
 * {@link org.infinispan.BaseCacheStream} operations to the provided {@link CacheStream}. This allows for intercepting
 * methods defined on <b>BaseCacheStream</b>.
 * <p>
 * This class is package private as it should only be created by using a map operator from another
 * AbstractDelegating*CacheStream instance. Note that {@link AbstractDelegatingCacheStream} is public as this is
 * the defined approach to create such a delegated stream.
 * @author wburns
 * @since 9.2
 */
class AbstractDelegatingLongCacheStream implements LongCacheStream {
   protected AbstractDelegatingCacheStream<?> delegateCacheStream;
   protected LongCacheStream underlyingStream;

   AbstractDelegatingLongCacheStream(AbstractDelegatingCacheStream<?> delegateCacheStream,
         LongCacheStream underlyingStream) {
      this.delegateCacheStream = delegateCacheStream;
      this.underlyingStream = underlyingStream;
   }

   // These are methods that convert to a different AbstractDelegating*CacheStream

   @Override
   public IntCacheStream mapToInt(LongToIntFunction mapper) {
      return underlyingStream.mapToInt(mapper);
   }

   @Override
   public DoubleCacheStream mapToDouble(LongToDoubleFunction mapper) {
      return new AbstractDelegatingDoubleCacheStream(delegateCacheStream, underlyingStream.mapToDouble(mapper));
   }

   @Override
   public <U> CacheStream<U> mapToObj(LongFunction<? extends U> mapper) {
      delegateCacheStream.underlyingStream = underlyingStream.mapToObj(mapper);
      return (CacheStream<U>) delegateCacheStream;
   }

   @Override
   public CacheStream<Long> boxed() {
      delegateCacheStream.underlyingStream = underlyingStream.boxed();
      return (CacheStream<Long>) delegateCacheStream;
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return new AbstractDelegatingDoubleCacheStream(delegateCacheStream, underlyingStream.asDoubleStream());
   }

   // These are methods that should delegate to the original cache stream

   @Override
   public LongCacheStream sequentialDistribution() {
      delegateCacheStream = delegateCacheStream.sequentialDistribution();
      return this;
   }

   @Override
   public LongCacheStream parallelDistribution() {
      delegateCacheStream = delegateCacheStream.parallelDistribution();
      return this;
   }

   @Override
   public LongCacheStream filterKeySegments(Set<Integer> segments) {
      delegateCacheStream = delegateCacheStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public LongCacheStream filterKeySegments(IntSet segments) {
      delegateCacheStream = delegateCacheStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public LongCacheStream filterKeys(Set<?> keys) {
      delegateCacheStream = delegateCacheStream.filterKeys(keys);
      return this;
   }

   @Override
   public LongCacheStream distributedBatchSize(int batchSize) {
      delegateCacheStream = delegateCacheStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public LongCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      delegateCacheStream = delegateCacheStream.segmentCompletionListener(listener);
      return this;
   }

   @Override
   public LongCacheStream disableRehashAware() {
      delegateCacheStream = delegateCacheStream.disableRehashAware();
      return this;
   }

   @Override
   public LongCacheStream timeout(long timeout, TimeUnit unit) {
      delegateCacheStream = delegateCacheStream.timeout(timeout, unit);
      return this;
   }

   // Actual LongStream operations

   @Override
   public LongCacheStream filter(LongPredicate predicate) {
      underlyingStream = underlyingStream.filter(predicate);
      return this;
   }

   @Override
   public LongCacheStream map(LongUnaryOperator mapper) {
      underlyingStream = underlyingStream.map(mapper);
      return this;
   }

   @Override
   public LongCacheStream flatMap(LongFunction<? extends LongStream> mapper) {
      underlyingStream = underlyingStream.flatMap(mapper);
      return this;
   }

   @Override
   public LongCacheStream distinct() {
      underlyingStream = underlyingStream.distinct();
      return this;
   }

   @Override
   public LongCacheStream sorted() {
      underlyingStream = underlyingStream.sorted();
      return this;
   }

   @Override
   public LongCacheStream peek(LongConsumer action) {
      underlyingStream = underlyingStream.peek(action);
      return this;
   }

   @Override
   public LongCacheStream limit(long maxSize) {
      underlyingStream = underlyingStream.limit(maxSize);
      return this;
   }

   @Override
   public LongCacheStream skip(long n) {
      underlyingStream = underlyingStream.skip(n);
      return this;
   }

   @Override
   public void forEach(LongConsumer action) {
      underlyingStream.forEach(action);
   }

   @Override
   public void forEachOrdered(LongConsumer action) {
      underlyingStream.forEachOrdered(action);
   }

   @Override
   public long[] toArray() {
      return underlyingStream.toArray();
   }

   @Override
   public long reduce(long identity, LongBinaryOperator op) {
      return underlyingStream.reduce(identity, op);
   }

   @Override
   public OptionalLong reduce(LongBinaryOperator op) {
      return underlyingStream.reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return underlyingStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public long sum() {
      return underlyingStream.sum();
   }

   @Override
   public OptionalLong min() {
      return underlyingStream.min();
   }

   @Override
   public OptionalLong max() {
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
   public LongSummaryStatistics summaryStatistics() {
      return underlyingStream.summaryStatistics();
   }

   @Override
   public boolean anyMatch(LongPredicate predicate) {
      return underlyingStream.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return underlyingStream.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return underlyingStream.noneMatch(predicate);
   }

   @Override
   public OptionalLong findFirst() {
      return underlyingStream.findFirst();
   }

   @Override
   public OptionalLong findAny() {
      return underlyingStream.findAny();
   }

   @Override
   public <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action) {
      underlyingStream.forEach(action);
   }

   @Override
   public LongCacheStream sequential() {
      underlyingStream = underlyingStream.sequential();
      return this;
   }

   @Override
   public LongCacheStream parallel() {
      underlyingStream = underlyingStream.parallel();
      return this;
   }

   @Override
   public PrimitiveIterator.OfLong iterator() {
      return underlyingStream.iterator();
   }

   @Override
   public Spliterator.OfLong spliterator() {
      return underlyingStream.spliterator();
   }

   @Override
   public boolean isParallel() {
      return underlyingStream.isParallel();
   }

   @Override
   public LongCacheStream unordered() {
      underlyingStream = underlyingStream.unordered();
      return this;
   }

   @Override
   public LongCacheStream onClose(Runnable closeHandler) {
      underlyingStream = underlyingStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      underlyingStream.close();
   }
}
