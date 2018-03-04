package org.infinispan.stream.impl;

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
import org.infinispan.stream.impl.local.LocalDoubleCacheStream;

/**
 * An intermediate double cache stream used when an intermediate operation that requires both a remote and local portion
 */
public class IntermediateDoubleCacheStream implements DoubleCacheStream {
   private BaseCacheStream remoteStream;
   private final IntermediateType type;
   private LocalDoubleCacheStream localDoubleStream;
   private IntermediateCacheStreamSupplier supplier;

   public IntermediateDoubleCacheStream(DistributedDoubleCacheStream remoteStream) {
      this.remoteStream = remoteStream;
      this.type = IntermediateType.DOUBLE;
      this.supplier = new IntermediateCacheStreamSupplier(type, remoteStream);
      this.localDoubleStream = new LocalDoubleCacheStream(supplier, remoteStream.parallel,
              remoteStream.registry);
   }

   public IntermediateDoubleCacheStream(BaseCacheStream remoteStream, IntermediateType type,
           LocalDoubleCacheStream localDoubleStream, IntermediateCacheStreamSupplier supplier) {
      this.remoteStream = remoteStream;
      this.type = type;
      this.localDoubleStream = localDoubleStream;
      this.supplier = supplier;
   }

   @Override
   public DoubleCacheStream sequentialDistribution() {
      remoteStream = remoteStream.sequentialDistribution();
      return this;
   }

   @Override
   public DoubleCacheStream parallelDistribution() {
      remoteStream = remoteStream.parallelDistribution();
      return this;
   }

   @Override
   public DoubleCacheStream filterKeySegments(Set<Integer> segments) {
      remoteStream = remoteStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public DoubleCacheStream filterKeySegments(IntSet segments) {
      remoteStream = remoteStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public DoubleCacheStream filterKeys(Set<?> keys) {
      remoteStream = remoteStream.filterKeys(keys);
      return this;
   }

   @Override
   public DoubleCacheStream distributedBatchSize(int batchSize) {
      remoteStream = remoteStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public DoubleCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("Segment completion listener is only supported when no intermediate " +
              "operation is provided (sorted, distinct, limit, skip)");
   }

   @Override
   public DoubleCacheStream disableRehashAware() {
      remoteStream = remoteStream.disableRehashAware();
      return this;
   }

   @Override
   public DoubleCacheStream timeout(long timeout, TimeUnit unit) {
      remoteStream = remoteStream.timeout(timeout, unit);
      return this;
   }

   @Override
   public boolean isParallel() {
      return localDoubleStream.isParallel();
   }

   @Override
   public DoubleCacheStream sorted() {
      localDoubleStream = localDoubleStream.sorted();
      return this;
   }

   @Override
   public DoubleCacheStream limit(long maxSize) {
      localDoubleStream = localDoubleStream.limit(maxSize);
      return this;
   }

   @Override
   public DoubleCacheStream skip(long n) {
      localDoubleStream = localDoubleStream.skip(n);
      return this;
   }

   @Override
   public DoubleCacheStream peek(DoubleConsumer action) {
      localDoubleStream = localDoubleStream.peek(action);
      return this;
   }

   @Override
   public DoubleCacheStream distinct() {
      localDoubleStream = localDoubleStream.distinct();
      return this;
   }

   @Override
   public DoubleCacheStream filter(DoublePredicate predicate) {
      localDoubleStream = localDoubleStream.filter(predicate);
      return this;
   }

   @Override
   public DoubleCacheStream map(DoubleUnaryOperator mapper) {
      localDoubleStream.map(mapper);
      return this;
   }

   @Override
   public <U> CacheStream<U> mapToObj(DoubleFunction<? extends U> mapper) {
      return new IntermediateCacheStream<>(remoteStream, type, localDoubleStream.mapToObj(mapper), supplier);
   }

   @Override
   public IntCacheStream mapToInt(DoubleToIntFunction mapper) {
      return new IntermediateIntCacheStream(remoteStream, type, localDoubleStream.mapToInt(mapper), supplier);
   }

   @Override
   public LongCacheStream mapToLong(DoubleToLongFunction mapper) {
      return new IntermediateLongCacheStream(remoteStream, type, localDoubleStream.mapToLong(mapper), supplier);
   }

   @Override
   public DoubleCacheStream flatMap(DoubleFunction<? extends DoubleStream> mapper) {
      localDoubleStream.flatMap(mapper);
      return this;
   }

   @Override
   public DoubleCacheStream parallel() {
      remoteStream = (BaseCacheStream) remoteStream.parallel();
      localDoubleStream = (LocalDoubleCacheStream) localDoubleStream.parallel();
      return this;
   }

   @Override
   public PrimitiveIterator.OfDouble iterator() {
      return localDoubleStream.iterator();
   }

   @Override
   public Spliterator.OfDouble spliterator() {
      return localDoubleStream.spliterator();
   }

   @Override
   public DoubleCacheStream sequential() {
      remoteStream = (BaseCacheStream) remoteStream.sequential();
      localDoubleStream = (LocalDoubleCacheStream) localDoubleStream.sequential();
      return this;
   }

   @Override
   public DoubleCacheStream unordered() {
      localDoubleStream = (LocalDoubleCacheStream) localDoubleStream.unordered();
      return this;
   }

   @Override
   public void forEach(DoubleConsumer action) {
      localDoubleStream.forEach(action);
   }

   @Override
   public <K, V> void forEach(ObjDoubleConsumer<Cache<K, V>> action) {
      localDoubleStream.forEach(action);
   }

   @Override
   public void forEachOrdered(DoubleConsumer action) {
      localDoubleStream.forEachOrdered(action);
   }

   @Override
   public double[] toArray() {
      return localDoubleStream.toArray();
   }

   @Override
   public double reduce(double identity, DoubleBinaryOperator op) {
      return localDoubleStream.reduce(identity, op);
   }

   @Override
   public OptionalDouble reduce(DoubleBinaryOperator op) {
      return localDoubleStream.reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjDoubleConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return localDoubleStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public double sum() {
      return localDoubleStream.sum();
   }

   @Override
   public OptionalDouble min() {
      return localDoubleStream.min();
   }

   @Override
   public OptionalDouble max() {
      return localDoubleStream.max();
   }

   @Override
   public long count() {
      return localDoubleStream.count();
   }

   @Override
   public OptionalDouble average() {
      return localDoubleStream.average();
   }

   @Override
   public DoubleSummaryStatistics summaryStatistics() {
      return localDoubleStream.summaryStatistics();
   }

   @Override
   public boolean anyMatch(DoublePredicate predicate) {
      return localDoubleStream.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(DoublePredicate predicate) {
      return localDoubleStream.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(DoublePredicate predicate) {
      return localDoubleStream.noneMatch(predicate);
   }

   @Override
   public OptionalDouble findFirst() {
      return localDoubleStream.findFirst();
   }

   @Override
   public OptionalDouble findAny() {
      return localDoubleStream.findAny();
   }

   @Override
   public CacheStream<Double> boxed() {
      return mapToObj(Double::valueOf);
   }

   @Override
   public DoubleCacheStream onClose(Runnable closeHandler) {
      remoteStream = (BaseCacheStream) remoteStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      localDoubleStream.close();
      remoteStream.close();
   }
}
