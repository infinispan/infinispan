package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
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
   protected final ComponentRegistry registry;

   protected final Collection<Runnable> onCloseRunnables = new ArrayList<>(4);

   protected Set<Integer> segmentsToFilter;
   protected Set<?> keysToFilter;

   protected Stream<R> stream;

   /**
    *  @param parallel
    * @param hash
    * @param supplier This must be a supplier that provides
    * @param registry
    */
   public AbstractLocalCacheStream(boolean parallel, ConsistentHash hash,
           Supplier<Stream<CacheEntry<K, V>>> supplier, ComponentRegistry registry) {
      this.parallel = parallel;
      this.hash = hash;
      this.supplier = supplier;
      this.registry = registry;
   }

   ConsistentHash getHash() {
      return hash;
   }

   Supplier<Stream<CacheEntry<K, V>>> getSupplier() {
      return supplier;
   }

   protected abstract Stream<R> getStream();

   private final Stream<R> getOrCreateStream() {
      if (stream == null) {
         stream = getStream();
         for (Runnable runnable : onCloseRunnables) {
            stream = stream.onClose(runnable);
         }
      }
      return stream;
   }

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
      registry.wireDependencies(predicate);
      stream = getOrCreateStream().filter(predicate);
      return this;
   }

   @Override
   public <R1> Stream<R1> map(Function<? super R, ? extends R1> mapper) {
      registry.wireDependencies(mapper);
      stream = (Stream<R>) getOrCreateStream().map(mapper);
      return (Stream<R1>) this;
   }

   @Override
   public IntStream mapToInt(ToIntFunction<? super R> mapper) {
      return getOrCreateStream().mapToInt(mapper);
   }

   @Override
   public LongStream mapToLong(ToLongFunction<? super R> mapper) {
      return getOrCreateStream().mapToLong(mapper);
   }

   @Override
   public DoubleStream mapToDouble(ToDoubleFunction<? super R> mapper) {
      return getOrCreateStream().mapToDouble(mapper);
   }

   @Override
   public <R1> Stream<R1> flatMap(Function<? super R, ? extends Stream<? extends R1>> mapper) {
      stream = (Stream<R>) getOrCreateStream().flatMap(mapper);
      return (Stream<R1>) this;
   }

   @Override
   public IntStream flatMapToInt(Function<? super R, ? extends IntStream> mapper) {
      return getOrCreateStream().flatMapToInt(mapper);
   }

   @Override
   public LongStream flatMapToLong(Function<? super R, ? extends LongStream> mapper) {
      return getOrCreateStream().flatMapToLong(mapper);
   }

   @Override
   public DoubleStream flatMapToDouble(Function<? super R, ? extends DoubleStream> mapper) {
      return getOrCreateStream().flatMapToDouble(mapper);
   }

   @Override
   public Stream<R> distinct() {
      stream = getOrCreateStream().distinct();
      return this;
   }

   @Override
   public Stream<R> sorted() {
      stream = getOrCreateStream().sorted();
      return this;
   }

   @Override
   public Stream<R> sorted(Comparator<? super R> comparator) {
      stream = getOrCreateStream().sorted(comparator);
      return this;
   }

   @Override
   public Stream<R> peek(Consumer<? super R> action) {
      stream = getOrCreateStream().peek(action);
      return this;
   }

   @Override
   public Stream<R> limit(long maxSize) {
      stream = getOrCreateStream().limit(maxSize);
      return this;
   }

   @Override
   public Stream<R> skip(long n) {
      stream = getOrCreateStream().skip(n);
      return this;
   }

   @Override
   public void forEach(Consumer<? super R> action) {
      getOrCreateStream().forEach(action);
   }

   @Override
   public void forEachOrdered(Consumer<? super R> action) {
      getOrCreateStream().forEachOrdered(action);
   }

   @Override
   public Object[] toArray() {
      return getOrCreateStream().toArray();
   }

   @Override
   public <A> A[] toArray(IntFunction<A[]> generator) {
      return getOrCreateStream().toArray(generator);
   }

   @Override
   public R reduce(R identity, BinaryOperator<R> accumulator) {
      return getOrCreateStream().reduce(identity, accumulator);
   }

   @Override
   public Optional<R> reduce(BinaryOperator<R> accumulator) {
      return getOrCreateStream().reduce(accumulator);
   }

   @Override
   public <U> U reduce(U identity, BiFunction<U, ? super R, U> accumulator, BinaryOperator<U> combiner) {
      return getOrCreateStream().reduce(identity, accumulator, combiner);
   }

   @Override
   public <R1> R1 collect(Supplier<R1> supplier, BiConsumer<R1, ? super R> accumulator, BiConsumer<R1, R1> combiner) {
      return getOrCreateStream().collect(supplier, accumulator, combiner);
   }

   @Override
   public <R1, A> R1 collect(Collector<? super R, A, R1> collector) {
      return getOrCreateStream().collect(collector);
   }

   @Override
   public Optional<R> min(Comparator<? super R> comparator) {
      return getOrCreateStream().min(comparator);
   }

   @Override
   public Optional<R> max(Comparator<? super R> comparator) {
      return getOrCreateStream().max(comparator);
   }

   @Override
   public long count() {
      return getOrCreateStream().count();
   }

   @Override
   public boolean anyMatch(Predicate<? super R> predicate) {
      return getOrCreateStream().anyMatch(predicate);
   }

   @Override
   public boolean allMatch(Predicate<? super R> predicate) {
      return getOrCreateStream().allMatch(predicate);
   }

   @Override
   public boolean noneMatch(Predicate<? super R> predicate) {
      return getOrCreateStream().noneMatch(predicate);
   }

   @Override
   public Optional<R> findFirst() {
      return getOrCreateStream().findFirst();
   }

   @Override
   public Optional<R> findAny() {
      return getOrCreateStream().findAny();
   }

   @Override
   public CloseableIterator<R> iterator() {
      return Closeables.iterator(getOrCreateStream());
   }

   @Override
   public Spliterator<R> spliterator() {
      return getOrCreateStream().spliterator();
   }

   @Override
   public boolean isParallel() {
      return getOrCreateStream().isParallel();
   }

   @Override
   public Stream<R> sequential() {
      stream = getOrCreateStream().sequential();
      return this;
   }

   @Override
   public Stream<R> parallel() {
      stream = getOrCreateStream().parallel();
      return this;
   }

   @Override
   public Stream<R> unordered() {
      stream = getOrCreateStream().unordered();
      return this;
   }

   @Override
   public Stream<R> onClose(Runnable closeHandler) {
      onCloseRunnables.add(closeHandler);
      return this;
   }

   @Override
   public void close() {
      getOrCreateStream().close();
   }
}
