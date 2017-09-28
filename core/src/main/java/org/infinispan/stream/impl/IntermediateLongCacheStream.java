package org.infinispan.stream.impl;

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

import org.infinispan.BaseCacheStream;
import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.DoubleCacheStream;
import org.infinispan.IntCacheStream;
import org.infinispan.LongCacheStream;
import org.infinispan.stream.impl.local.LocalLongCacheStream;

/**
 * An intermediate long cache stream used when an intermediate operation that requires both a remote and local portion
 */
public class IntermediateLongCacheStream implements LongCacheStream {
   private BaseCacheStream remoteStream;
   private final IntermediateType type;
   private LocalLongCacheStream localLongStream;
   private IntermediateCacheStreamSupplier supplier;

   public IntermediateLongCacheStream(DistributedLongCacheStream remoteStream) {
      this.remoteStream = remoteStream;
      this.type = IntermediateType.LONG;
      this.supplier = new IntermediateCacheStreamSupplier(type, remoteStream);
      this.localLongStream = new LocalLongCacheStream(supplier, remoteStream.parallel,
              remoteStream.registry);
   }

   public IntermediateLongCacheStream(BaseCacheStream remoteStream, IntermediateType type,
           LocalLongCacheStream localLongStream, IntermediateCacheStreamSupplier supplier) {
      this.remoteStream = remoteStream;
      this.type = type;
      this.localLongStream = localLongStream;
      this.supplier = supplier;
   }

   @Override
   public LongCacheStream sequentialDistribution() {
      remoteStream = remoteStream.sequentialDistribution();
      return this;
   }

   @Override
   public LongCacheStream parallelDistribution() {
      remoteStream = remoteStream.parallelDistribution();
      return this;
   }

   @Override
   public LongCacheStream filterKeySegments(Set<Integer> segments) {
      remoteStream = remoteStream.filterKeySegments(segments);
      return this;
   }

   @Override
   public LongCacheStream filterKeys(Set<?> keys) {
      remoteStream = remoteStream.filterKeys(keys);
      return this;
   }

   @Override
   public LongCacheStream distributedBatchSize(int batchSize) {
      remoteStream = remoteStream.distributedBatchSize(batchSize);
      return this;
   }

   @Override
   public LongCacheStream segmentCompletionListener(SegmentCompletionListener listener) {
      throw new UnsupportedOperationException("Segment completion listener is only supported when no intermediate " +
              "operation is provided (sorted, distinct, limit, skip)");
   }

   @Override
   public LongCacheStream disableRehashAware() {
      remoteStream = remoteStream.disableRehashAware();
      return this;
   }

   @Override
   public LongCacheStream timeout(long timeout, TimeUnit unit) {
      remoteStream = remoteStream.timeout(timeout, unit);
      return this;
   }

   @Override
   public boolean isParallel() {
      return localLongStream.isParallel();
   }

   @Override
   public LongCacheStream sorted() {
      localLongStream = localLongStream.sorted();
      return this;
   }

   @Override
   public LongCacheStream limit(long maxSize) {
      localLongStream = localLongStream.limit(maxSize);
      return this;
   }

   @Override
   public LongCacheStream skip(long n) {
      localLongStream = localLongStream.skip(n);
      return this;
   }

   @Override
   public LongCacheStream peek(LongConsumer action) {
      localLongStream = localLongStream.peek(action);
      return this;
   }

   @Override
   public LongCacheStream distinct() {
      localLongStream = localLongStream.distinct();
      return this;
   }

   @Override
   public LongCacheStream filter(LongPredicate predicate) {
      localLongStream = localLongStream.filter(predicate);
      return this;
   }

   @Override
   public LongCacheStream map(LongUnaryOperator mapper) {
      localLongStream.map(mapper);
      return this;
   }

   @Override
   public <U> CacheStream<U> mapToObj(LongFunction<? extends U> mapper) {
      return new IntermediateCacheStream<>(remoteStream, type, localLongStream.mapToObj(mapper), supplier);
   }

   @Override
   public IntCacheStream mapToInt(LongToIntFunction mapper) {
      return new IntermediateIntCacheStream(remoteStream, type, localLongStream.mapToInt(mapper), supplier);
   }

   @Override
   public DoubleCacheStream mapToDouble(LongToDoubleFunction mapper) {
      return new IntermediateDoubleCacheStream(remoteStream, type, localLongStream.mapToDouble(mapper), supplier);
   }

   @Override
   public LongCacheStream flatMap(LongFunction<? extends LongStream> mapper) {
      localLongStream.flatMap(mapper);
      return this;
   }

   @Override
   public LongCacheStream parallel() {
      remoteStream = (BaseCacheStream) remoteStream.parallel();
      localLongStream = (LocalLongCacheStream) localLongStream.parallel();
      return this;
   }

   @Override
   public PrimitiveIterator.OfLong iterator() {
      return localLongStream.iterator();
   }

   @Override
   public Spliterator.OfLong spliterator() {
      return localLongStream.spliterator();
   }

   @Override
   public LongCacheStream sequential() {
      remoteStream = (BaseCacheStream) remoteStream.sequential();
      localLongStream = (LocalLongCacheStream) localLongStream.sequential();
      return this;
   }

   @Override
   public LongCacheStream unordered() {
      localLongStream = (LocalLongCacheStream) localLongStream.unordered();
      return this;
   }

   @Override
   public void forEach(LongConsumer action) {
      localLongStream.forEach(action);
   }

   @Override
   public <K, V> void forEach(ObjLongConsumer<Cache<K, V>> action) {
      localLongStream.forEach(action);
   }

   @Override
   public void forEachOrdered(LongConsumer action) {
      localLongStream.forEachOrdered(action);
   }

   @Override
   public long[] toArray() {
      return localLongStream.toArray();
   }

   @Override
   public long reduce(long identity, LongBinaryOperator op) {
      return localLongStream.reduce(identity, op);
   }

   @Override
   public OptionalLong reduce(LongBinaryOperator op) {
      return localLongStream.reduce(op);
   }

   @Override
   public <R> R collect(Supplier<R> supplier, ObjLongConsumer<R> accumulator, BiConsumer<R, R> combiner) {
      return localLongStream.collect(supplier, accumulator, combiner);
   }

   @Override
   public long sum() {
      return localLongStream.sum();
   }

   @Override
   public OptionalLong min() {
      return localLongStream.min();
   }

   @Override
   public OptionalLong max() {
      return localLongStream.max();
   }

   @Override
   public long count() {
      return localLongStream.count();
   }

   @Override
   public OptionalDouble average() {
      return localLongStream.average();
   }

   @Override
   public LongSummaryStatistics summaryStatistics() {
      return localLongStream.summaryStatistics();
   }

   @Override
   public boolean anyMatch(LongPredicate predicate) {
      return localLongStream.anyMatch(predicate);
   }

   @Override
   public boolean allMatch(LongPredicate predicate) {
      return localLongStream.allMatch(predicate);
   }

   @Override
   public boolean noneMatch(LongPredicate predicate) {
      return localLongStream.noneMatch(predicate);
   }

   @Override
   public OptionalLong findFirst() {
      return localLongStream.findFirst();
   }

   @Override
   public OptionalLong findAny() {
      return localLongStream.findAny();
   }

   @Override
   public CacheStream<Long> boxed() {
      return mapToObj(Long::valueOf);
   }

   @Override
   public DoubleCacheStream asDoubleStream() {
      return mapToDouble(l -> (double) l);
   }

   @Override
   public LongCacheStream onClose(Runnable closeHandler) {
      remoteStream = (BaseCacheStream) remoteStream.onClose(closeHandler);
      return this;
   }

   @Override
   public void close() {
      localLongStream.close();
      remoteStream.close();
   }
}
