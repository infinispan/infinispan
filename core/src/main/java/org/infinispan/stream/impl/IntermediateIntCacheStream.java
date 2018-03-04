package org.infinispan.stream.impl;

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

import org.infinispan.BaseCacheStream;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.stream.impl.local.LocalIntCacheStream;

/**
 * An intermediate int cache stream used when an intermediate operation that requires both a remote and local portion
 */
public class IntermediateIntCacheStream implements IntCacheStream {
   private BaseCacheStream remoteStream;
   private final IntermediateType type;
   private LocalIntCacheStream localIntStream;
   private IntermediateCacheStreamSupplier supplier;

   public IntermediateIntCacheStream(DistributedIntCacheStream remoteStream) {
      this.remoteStream = remoteStream;
      this.type = IntermediateType.INT;
      this.supplier = new IntermediateCacheStreamSupplier(type, remoteStream);
      this.localIntStream = new LocalIntCacheStream(supplier, remoteStream.parallel,
              remoteStream.registry);
   }

   public IntermediateIntCacheStream(BaseCacheStream remoteStream, IntermediateType type,
           LocalIntCacheStream localIntStream, IntermediateCacheStreamSupplier supplier) {
      this.remoteStream = remoteStream;
      this.type = type;
      this.localIntStream = localIntStream;
      this.supplier = supplier;
   }

   @Override
   public IntCacheStream sequentialDistribution() {
      remoteStream = remoteStream.sequentialDistribution();
      return this;
   }

   @Override
   public IntCacheStream parallelDistribution() {
      remoteStream = remoteStream.parallelDistribution();
      return this;
   }

   @Override
   public IntCacheStream filterKeySegments(Set<Integer> segments) {
      remoteStream = remoteStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public IntCacheStream filterKeySegments(IntSet segments) {
      remoteStream = remoteStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public IntCacheStream filterKeys(Set<?> keys) {
      remoteStream = remoteStream.filterKeys(keys);
      return this;
   }

   @Override
   public IntCacheStream distributedBatchSize(int batchSize) {
      remoteStream = remoteStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public IntCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("Segment completion listener is only supported when no intermediate " +
              "operation is provided (sorted, distinct, limit, skip)");
   }

   @Override
   public IntCacheStream disableRehashAware() {
      remoteStream = remoteStream.disableRehashAware();
      return this;
   }

   @Override
   public IntCacheStream timeout(long timeout, TimeUnit unit) {
      remoteStream = remoteStream.timeout(timeout, unit);
      return this;
   }

   @Override
   public boolean isParallel() {
      return localIntStream.isParallel();
   }

   @Override
   public IntCacheStream sorted() {
      localIntStream = localIntStream.sorted();
      return this;
   }

   @Override
   public IntCacheStream limit(long maxSize) {
      localIntStream = localIntStream.limit(maxSize);
      return this;
   }

   @Override
   public IntCacheStream skip(long n) {
      localIntStream = localIntStream.skip(n);
      return this;
   }

   @Override
   public IntCacheStream peek(IntConsumer action) {
      localIntStream = localIntStream.peek(action);
      return this;
   }

   @Override
   public IntCacheStream distinct() {
      localIntStream = localIntStream.distinct();
      return this;
   }

   @Override
   public IntCacheStream filter(IntPredicate predicate) {
      localIntStream = localIntStream.filter(predicate);
      return this;
   }

   @Override
   public IntCacheStream map(IntUnaryOperator mapper) {
      localIntStream.map(mapper);
      return this;
   }

   @Override
   public <U> CacheStream<U> mapToObj(IntFunction<? extends U> mapper) {
      return new IntermediateCacheStream<>(remoteStream, type, localIntStream.mapToObj(mapper), supplier);
   }

   @Override
   public LongCacheStream mapToLong(IntToLongFunction mapper) {
      return new IntermediateLongCacheStream(remoteStream, type, localIntStream.mapToLong(mapper), supplier);
   }

   @Override
   public DoubleCacheStream mapToDouble(IntToDoubleFunction mapper) {
      return new IntermediateDoubleCacheStream(remoteStream, type, localIntStream.mapToDouble(mapper), supplier);
   }

   @Override
   public IntCacheStream flatMap(IntFunction<? extends IntStream> mapper) {
      localIntStream.flatMap(mapper);
      return this;
   }

   @Override
   public IntCacheStream parallel() {
      remoteStream = (BaseCacheStream) remoteStream.parallel();
      localIntStream = (LocalIntCacheStream) localIntStream.parallel();
      return this;
   }

   @Override
   public PrimitiveIterator.OfInt iterator() {
      return localIntStream.iterator();
   }

   @Override
   public Spliterator.OfInt spliterator() {
      return localIntStream.spliterator();
   }

   @Override
   public IntCacheStream sequential() {
      remoteStream = (BaseCacheStream) remoteStream.sequential();
      localIntStream = (LocalIntCacheStream) localIntStream.sequential();
      return this;
   }

   @Override
   public IntCacheStream unordered() {
      localIntStream = (LocalIntCacheStream) localIntStream.unordered();
      return this;
   }

   @Override
   public void forEach(IntConsumer action) {
      localIntStream.forEach(action);
   }

   @Override
   public <K, V> void forEach(ObjIntConsumer<Cache<K, V>> action) {
      localIntStream.forEach(action);
   }

   @Override
   public void forEachOrdered(IntConsumer action) {
      localIntStream.forEachOrdered(action);
   }


   @Override
   public int[] toArray() {
      return localIntStream.toArray();
   }

   @Override
   public int reduce(int identity, IntBinaryOperator op) {
      return localIntStream.reduce(identity, op);
   }

   @Override
   public OptionalInt reduce(IntBinaryOperator op) {
      return localIntStream.reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjIntConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return localIntStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public int sum() {
      return localIntStream.sum();
   }

   @Override
   public OptionalInt min() {
      return localIntStream.min();
   }

   @Override
   public OptionalInt max() {
      return localIntStream.max();
   }

   @Override
   public long count() {
      return localIntStream.count();
   }

   @Override
   public OptionalDouble average() {
      return localIntStream.average();
   }

   @Override
   public IntSummaryStatistics summaryStatistics() {
      return localIntStream.summaryStatistics();
   }

   @Override
   public boolean anyMatch(IntPredicate predicate) {
      return localIntStream.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(IntPredicate predicate) {
      return localIntStream.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(IntPredicate predicate) {
      return localIntStream.noneMatch(predicate);
   }

   @Override
   public OptionalInt findFirst() {
      return localIntStream.findFirst();
   }

   @Override
   public OptionalInt findAny() {
      return localIntStream.findAny();
   }

   @Override
   public CacheStream<Integer> boxed() {
      return mapToObj(Integer::valueOf);
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return mapToDouble(l -> (double) l);
   }

   @Override
   public LongCacheStream asLongStream() {
      return mapToLong(l -> (long) l);
   }

   @Override
   public IntCacheStream onClose(Runnable closeHandler) {
      remoteStream = (BaseCacheStream) remoteStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      localIntStream.close();
      remoteStream.close();
   }
}
