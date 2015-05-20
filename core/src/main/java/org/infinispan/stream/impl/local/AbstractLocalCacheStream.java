package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Implements the majority of operations required for a local cache stream.  The only method is the underlying
 * {@link AbstractLocalCacheStream#getStream()} which is useful for different implementations to tweak how the
 * stream is populated
 * @param <R> type of the stream
 * @param <K> key type of the supplied cache stream
 * @param <V> value type of the supplied cache stream
 */
public abstract class AbstractLocalCacheStream<R, K, V> implements CacheStream<R> {
   protected final Log log = LogFactory.getLog(getClass());

   protected final boolean parallel;
   protected final ConsistentHash hash;
   protected final Supplier<Stream<CacheEntry<K, V>>> supplier;

   protected Set<Integer> segmentsToFilter;
   protected Set<?> keysToFilter;

   /**
    *
    * @param parallel
    * @param hash
    * @param supplier This must be a supplier that provides
    */
   public AbstractLocalCacheStream(boolean parallel, ConsistentHash hash,
                                   Supplier<Stream<CacheEntry<K, V>>> supplier) {
      this.parallel = parallel;
      this.hash = hash;
      this.supplier = supplier;
   }

   ConsistentHash getHash() {
      return hash;
   }

   Supplier<Stream<CacheEntry<K, V>>> getSupplier() {
      return supplier;
   }

   protected abstract Stream<R> getStream();

   @Override
   public CacheStream<R> sequentialDistribution() {
      return this;
   }

   @Override
   public CacheStream<R> parallelDistribution() {
      return this;
   }

   @Override
   public CacheStream<R> filterKeySegments(Set<Integer> segments) {
      segmentsToFilter = segments;
      return this;
   }

   @Override
   public CacheStream<R> filterKeys(Set<?> keys) {
      keysToFilter = keys;
      return this;
   }

   @Override
   public CacheStream<R> distributedBatchSize(int batchSize) {
      // TODO: Does this change cache loader?
      return this;
   }

   @Override
   public CacheStream<R> segmentCompletionListener(SegmentCompletionListener listener) {
      // All segments are completed when the getStream() is completed so we don't track them
      return this;
   }

   @Override
   public CacheStream<R> disableRehashAware() {
      // Local stream doesn't matter for rehash
      return this;
   }

   @Override
   public Stream<R> filter(Predicate<? super R> predicate) {
      return getStream().filter(predicate);
   }

   @Override
   public <R1> Stream<R1> map(Function<? super R, ? extends R1> mapper) {
      return getStream().map(mapper);
   }

   @Override
   public IntStream mapToInt(ToIntFunction<? super R> mapper) {
      return getStream().mapToInt(mapper);
   }

   @Override
   public LongStream mapToLong(ToLongFunction<? super R> mapper) {
      return getStream().mapToLong(mapper);
   }

   @Override
   public DoubleStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      return getStream().mapToDouble(mapper);
   }

   @Override
   public <R1> Stream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      return getStream().flatMap(mapper);
   }

   @Override
   public IntStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      return getStream().flatMapToInt(mapper);
   }

   @Override
   public LongStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      return getStream().flatMapToLong(mapper);
   }

   @Override
   public DoubleStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      return getStream().flatMapToDouble(mapper);
   }

   @Override
   public Stream<R> distinct() {
      return getStream().distinct();
   }

   @Override
   public Stream<R> sorted() {
      return getStream().sorted();
   }

   @Override
   public Stream<R> sorted(Comparator<? super R> comparator) {
      return getStream().sorted(comparator);
   }

   @Override
   public Stream<R> peek(Consumer<? super R> action) {
      return getStream().peek(action);
   }

   @Override
   public Stream<R> limit(long maxSize) {
      return getStream().limit(maxSize);
   }

   @Override
   public Stream<R> skip(long n) {
      return getStream().skip(n);
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      getStream().forEach(action);
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      getStream().forEachOrdered(action);
   }

   @Override
   public Object[] toArray() {
      return getStream().toArray();
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return getStream().toArray(generator);
   }

   @Override
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return getStream().reduce(identity, accumulator);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      return getStream().reduce(accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return getStream().reduce(identity, accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return getStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      return getStream().collect(collector);
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      return getStream().min(comparator);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      return getStream().max(comparator);
   }

   @Override
   public long count() {
      return getStream().count();
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return getStream().anyMatch(predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return getStream().allMatch(predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return getStream().noneMatch(predicate);
   }

   @Override
   public Optional<R> findFirst() {
      return getStream().findFirst();
   }

   @Override
   public Optional<R> findAny() {
      return getStream().findAny();
   }

   @Override
   public CloseableIterator<R> iterator() {
      return Closeables.iterator(getStream());
   }

   @Override
   public Spliterator<R> spliterator() {
      return getStream().spliterator();
   }

   @Override
   public boolean isParallel() {
      return getStream().isParallel();
   }

   @Override
   public Stream<R> sequential() {
      return getStream().sequential();
   }

   @Override
   public Stream<R> parallel() {
      return getStream().parallel();
   }

   @Override
   public Stream<R> unordered() {
      return getStream().unordered();
   }

   @Override
   public Stream<R> onClose(Runnable closeHandler) {
      return getStream().onClose(closeHandler);
   }

   @Override
   public void close() {
      getStream().close();
   }
}
