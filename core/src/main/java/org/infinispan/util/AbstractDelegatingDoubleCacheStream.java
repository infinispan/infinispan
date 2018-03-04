package org.infinispan.util;

import java.util.DoubleSummaryStatistics;
import java.util.OptionalDouble;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleToIntFunction;
import java.util.function.DoubleToLongFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;
import java.util.stream.DoubleStream;

import org.infinispan.BaseCacheStream;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;

/**
 * Abstract Delegating handler that passes DoubleStream operations off to the underlying DoubleCacheStream but delegates
 * {@link org.infinispan.BaseCacheStream} operations to the provided {@link CacheStream}. This allows for intercepting
 * methods defined on <b>BaseCacheStream</b>.
 * <p>
 * This class is package private as it should only be created by using a map operator from another
 * AbstractDelegating*CacheStream instance. Note that {@link AbstractDelegatingCacheStream} is public as this is
 * the defined approach to create such a delegated stream.
 * @author wburns
 * @since 9.2
 */
class AbstractDelegatingDoubleCacheStream implements DoubleCacheStream {
   protected AbstractDelegatingCacheStream<?> delegateCacheStream;
   protected DoubleCacheStream underlyingStream;

   AbstractDelegatingDoubleCacheStream(AbstractDelegatingCacheStream<?> delegateCacheStream,
         DoubleCacheStream underlyingStream) {
      this.delegateCacheStream = delegateCacheStream;
      this.underlyingStream = underlyingStream;
   }

   // These are methods that convert to a different AbstractDelegating*CacheStream

   @Override
   public IntCacheStream mapToInt(DoubleToIntFunction mapper) {
      return underlyingStream.mapToInt(mapper);
   }

   @Override
   public LongCacheStream mapToLong(DoubleToLongFunction mapper) {
      return new AbstractDelegatingLongCacheStream(delegateCacheStream, underlyingStream.mapToLong(mapper));
   }

   @Override
   public <U> CacheStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      delegateCacheStream.underlyingStream = underlyingStream.mapToObj(mapper);
      return (CacheStream<U>) delegateCacheStream;
   }

   @Override
   public CacheStream<Double> boxed() {
      delegateCacheStream.underlyingStream = underlyingStream.boxed();
      return (CacheStream<Double>) delegateCacheStream;
   }

   // These are methods that should delegate to the original cache stream

   @Override
   public DoubleCacheStream sequentialDistribution() {
      delegateCacheStream = delegateCacheStream.sequentialDistribution();
      return this;
   }

   @Override
   public DoubleCacheStream parallelDistribution() {
      delegateCacheStream = delegateCacheStream.parallelDistribution();
      return this;
   }

   @Override
   public DoubleCacheStream filterKeySegments(Set<Integer> segments) {
      delegateCacheStream = delegateCacheStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public BaseCacheStream filterKeySegments(IntSet segments) {
      delegateCacheStream = delegateCacheStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public DoubleCacheStream filterKeys(Set<?> keys) {
      delegateCacheStream = delegateCacheStream.filterKeys(keys);
      return this;
   }

   @Override
   public DoubleCacheStream distributedBatchSize(int batchSize) {
      delegateCacheStream = delegateCacheStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public DoubleCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      delegateCacheStream = delegateCacheStream.segmentCompletionListener(listener);
      return this;
   }

   @Override
   public DoubleCacheStream disableRehashAware() {
      delegateCacheStream = delegateCacheStream.disableRehashAware();
      return this;
   }

   @Override
   public DoubleCacheStream timeout(long timeout, TimeUnit unit) {
      delegateCacheStream = delegateCacheStream.timeout(timeout, unit);
      return this;
   }

   // Actual DoubleStream operations

   @Override
   public DoubleCacheStream filter(DoublePredicate predicate) {
      underlyingStream = underlyingStream.filter(predicate);
      return this;
   }

   @Override
   public DoubleCacheStream map(DoubleUnaryOperator mapper) {
      underlyingStream = underlyingStream.map(mapper);
      return this;
   }

   @Override
   public DoubleCacheStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      underlyingStream = underlyingStream.flatMap(mapper);
      return this;
   }

   @Override
   public DoubleCacheStream distinct() {
      underlyingStream = underlyingStream.distinct();
      return this;
   }

   @Override
   public DoubleCacheStream sorted() {
      underlyingStream = underlyingStream.sorted();
      return this;
   }

   @Override
   public DoubleCacheStream peek(DoubleConsumer action) {
      underlyingStream = underlyingStream.peek(action);
      return this;
   }

   @Override
   public DoubleCacheStream limit(long maxSize) {
      underlyingStream = underlyingStream.limit(maxSize);
      return this;
   }

   @Override
   public DoubleCacheStream skip(long n) {
      underlyingStream = underlyingStream.skip(n);
      return this;
   }

   @Override
   public void forEach(DoubleConsumer action) {
      underlyingStream.forEach(action);
   }

   @Override
   public void forEachOrdered(DoubleConsumer action) {
      underlyingStream.forEachOrdered(action);
   }

   @Override
   public double[] toArray() {
      return underlyingStream.toArray();
   }

   @Override
   public double reduce(double identity, DoubleBinaryOperator op) {
      return underlyingStream.reduce(identity, op);
   }

   @Override
   public OptionalDouble reduce(DoubleBinaryOperator op) {
      return underlyingStream.reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return underlyingStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public double sum() {
      return underlyingStream.sum();
   }

   @Override
   public OptionalDouble min() {
      return underlyingStream.min();
   }

   @Override
   public OptionalDouble max() {
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
   public DoubleSummaryStatistics summaryStatistics() {
      return underlyingStream.summaryStatistics();
   }

   @Override
   public boolean anyMatch(DoublePredicate predicate) {
      return underlyingStream.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return underlyingStream.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return underlyingStream.noneMatch(predicate);
   }

   @Override
   public OptionalDouble findFirst() {
      return underlyingStream.findFirst();
   }

   @Override
   public OptionalDouble findAny() {
      return underlyingStream.findAny();
   }

   @Override
   public <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action) {
      underlyingStream.forEach(action);
   }

   @Override
   public DoubleCacheStream sequential() {
      underlyingStream = underlyingStream.sequential();
      return this;
   }

   @Override
   public DoubleCacheStream parallel() {
      underlyingStream = underlyingStream.parallel();
      return this;
   }

   @Override
   public PrimitiveIterator.OfDouble iterator() {
      return underlyingStream.iterator();
   }

   @Override
   public Spliterator.OfDouble spliterator() {
      return underlyingStream.spliterator();
   }

   @Override
   public boolean isParallel() {
      return underlyingStream.isParallel();
   }

   @Override
   public DoubleCacheStream unordered() {
      underlyingStream = underlyingStream.unordered();
      return this;
   }

   @Override
   public DoubleCacheStream onClose(Runnable closeHandler) {
      underlyingStream = underlyingStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      underlyingStream.close();
   }
}
