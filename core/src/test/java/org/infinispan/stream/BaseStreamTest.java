package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.testng.AssertJUnit.*;

/**
 * Base test class for streams to verify proper behavior of all of the terminal operations for all of the various
 * stream classes
 */
@Test
public abstract class BaseStreamTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx;
   protected final CacheMode cacheMode;

   static final Map<Integer, Object> forEachStructure = new ConcurrentHashMap<>();
   static final AtomicInteger forEachOffset = new AtomicInteger();

   static int populateNextForEachStructure(Object obj) {
      int offset = forEachOffset.getAndIncrement();
      forEachStructure.put(offset, obj);
      return offset;
   }

   static <R> R getForEachObject(int offset) {
      return (R) forEachStructure.get(offset);
   }

   static void clearForEachObject(int offset) {
      forEachStructure.remove(offset);
   }


   public BaseStreamTest(boolean tx, CacheMode mode) {
      this.tx = tx;
      cacheMode = mode;
   }

   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      // Do nothing to config by default, used by people who extend this
   }

   protected abstract <E> CacheStream<E> createStream(CacheCollection<E> cacheCollection);

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().hash().numOwners(2);
         builderUsed.clustering().stateTransfer().chunkSize(50);
         enhanceConfiguration(builderUsed);
         createClusteredCaches(3, CACHE_NAME, builderUsed);
      } else {
         enhanceConfiguration(builderUsed);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builderUsed);
         cacheManagers.add(cm);
      }
   }

   protected <K, V> Cache<K, V> getCache(int index) {
      return cache(index, CACHE_NAME);
   }

   public void testObjAllMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).allMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getValue().endsWith("-value")));
      assertFalse(createStream(entrySet).allMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() % 2 == 0));
      assertTrue(createStream(entrySet).allMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() < 10 && e.getKey() >= 0));
      assertTrue(createStream(entrySet).allMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey().toString().equals(e.getValue().substring(0, 1))));
   }

   public void testObjAnyMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).anyMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getValue().endsWith("-value")));
      assertTrue(createStream(entrySet).anyMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() % 2 == 0));
      assertTrue(createStream(entrySet).anyMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() < 10 && e.getKey() >= 0));
      assertTrue(createStream(entrySet).anyMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getValue().equals("4-value")));
      assertFalse(createStream(entrySet).anyMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() > 12));
   }

   public void testObjCollectorIntAverager() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).collect(CacheCollectors.serializableCollector(
              () -> Collectors.averagingInt(e -> e.getKey()))));
   }

   public void testObjCollectorGroupBy() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

            ConcurrentMap<Boolean, List<Map.Entry<Integer, String>>> grouped = createStream(entrySet).collect(
                    CacheCollectors.serializableCollector(
                            () -> Collectors.groupingByConcurrent(k -> k.getKey() % 2 == 0)));
      grouped.get(true).parallelStream().forEach(e -> assertTrue(e.getKey() % 2 == 0));
      grouped.get(false).parallelStream().forEach(e -> assertTrue(e.getKey() % 2 == 1));
   }

   public void testObjCollect() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Map.Entry<Integer, String>> list = createStream(entrySet).collect((Serializable & Supplier<ArrayList>) ArrayList::new,
              (Serializable & BiConsumer<ArrayList, Map.Entry<Integer, String>>) ArrayList::add,
              (Serializable & BiConsumer<ArrayList, ArrayList>) ArrayList::addAll);
      assertEquals(cache.size(), list.size());
      list.parallelStream().forEach(e -> assertEquals(cache.get(e.getKey()), e.getValue()));
   }

   public void testObjSortedCollector() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Map.Entry<Integer, String>> list = createStream(entrySet).sorted(
              (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).collect(
              CacheCollectors.serializableCollector(() -> Collectors.toList()));
      assertEquals(cache.size(), list.size());
      AtomicInteger i = new AtomicInteger();
      list.forEach(e -> {
         assertEquals(i.getAndIncrement(), e.getKey().intValue());
         assertEquals(cache.get(e.getKey()), e.getValue());
      });
   }

   public void testObjCount() {
      Cache<Integer, String> cache = getCache(0);
      int range = 12;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(range, createStream(entrySet).count());
   }

   public void testObjFindAny() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).findAny().isPresent());
      assertTrue(createStream(entrySet).filter((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getValue().endsWith("-value")).findAny().isPresent());
      assertTrue(createStream(entrySet).filter((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).filter((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() < 10 && e.getKey() >= 0).findAny().isPresent());
      assertTrue(createStream(entrySet).filter((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getValue().equals("4-value")).findAny().isPresent());
      assertFalse(createStream(entrySet).filter((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() > 12).findAny().isPresent());
   }

   public void testObjFindFirst() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).sorted(
              (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).findFirst().get().getKey().intValue());
   }

   public void testObjForEach() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         createStream(entrySet).forEach(
                 (Serializable & Consumer<Map.Entry<Integer, String>>) e -> {
                    AtomicInteger atomic = getForEachObject(offset);
                    atomic.addAndGet(e.getKey());
                 });
         AtomicInteger atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2), atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   static ConcurrentLinkedQueue<String> SHARED_QUEUE = new ConcurrentLinkedQueue<>();

   public void testObjFlatMapForEach() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      SHARED_QUEUE.clear();
      createStream(entrySet).distributedBatchSize(5).flatMap(
              (Serializable & Function<Map.Entry<Integer, String>, Stream<String>>)
              e -> Arrays.stream(e.getValue().split("a"))).forEach(
              (Serializable & Consumer<String>) e -> SHARED_QUEUE.add(e));
      assertEquals(range * 2, SHARED_QUEUE.size());

      int lueCount = 0;
      for (String string : SHARED_QUEUE) {
         if (string.equals("lue")) lueCount++;
      }
      assertEquals(10, lueCount);
   }

   public void testObjForEachOrdered() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Map.Entry<Integer, String>> list = new ArrayList<>(range);
      // we sort inverted order
      createStream(entrySet).sorted((e1, e2) -> Integer.compare(e2.getKey(), e1.getKey())).forEachOrdered(
              list::add);
      assertEquals(range, list.size());
      for (int i = 0; i < range; ++i) {
         // 0 based so we have to also subtract by 1
         assertEquals(range - i - 1, list.get(i).getKey().intValue());
      }
   }

   public void testObjMax() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(Integer.valueOf(9),
              createStream(entrySet).max((Serializable & Comparator<Map.Entry<Integer, String>>)
                      (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).get().getKey());
   }

   public void testObjMin() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(Integer.valueOf(0),
              createStream(entrySet).min((Serializable & Comparator<Map.Entry<Integer, String>>)
                      (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).get().getKey());
   }

   public void testObjNoneMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).noneMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getValue().endsWith("-value")));
      assertFalse(createStream(entrySet).noneMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() % 2 == 0));
      assertFalse(createStream(entrySet).noneMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() < 10 && e.getKey() >= 0));
      assertFalse(createStream(entrySet).noneMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey().toString().equals(e.getValue().substring(0, 1))));
      assertTrue(createStream(entrySet).noneMatch((Serializable & Predicate<Map.Entry<Integer, String>>) e -> e.getKey() > 12));
   }

   public void testObjReduce1() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Optional<Map.Entry<Integer, String>> optional = createStream(entrySet).reduce(
              (Serializable & BinaryOperator<Map.Entry<Integer, String>>) (e1, e2) ->
                      new ImmortalCacheEntry(e1.getKey() + e2.getKey(), e1.getValue() + e2.getValue()));
      assertTrue(optional.isPresent());
      Map.Entry<Integer, String> result = optional.get();
      assertEquals((range - 1) * (range / 2), result.getKey().intValue());
      assertEquals(range * 7, result.getValue().length());
   }

   public void testObjReduce2() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Map.Entry<Integer, String> result = createStream(entrySet).reduce(new ImmortalCacheEntry(0, ""),
              (Serializable & BinaryOperator<Map.Entry<Integer, String>>) (e1, e2) ->
                      new ImmortalCacheEntry(e1.getKey() + e2.getKey(), e1.getValue() + e2.getValue()));
      assertEquals((range - 1) * (range / 2), result.getKey().intValue());
      assertEquals(range * 7, result.getValue().length());
   }

   public void testObjReduce2WithMap() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Integer result = createStream(entrySet).map(
              (Serializable & Function<Map.Entry<Integer, String>, Integer>) e -> e.getKey()
      ).reduce(0,
              (Serializable & BinaryOperator<Integer>) (e1, e2) -> e1 + e2);
      assertEquals((range - 1) * (range / 2), result.intValue());
   }

   public void testObjReduce3() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Integer result = createStream(entrySet).reduce(0,
              (Serializable & BiFunction<Integer, Map.Entry<Integer, String>, Integer>)
                      (e1, e2) ->
                              e1 + e2.getKey(),
              (Serializable & BinaryOperator<Integer>) (i1, i2) -> i1 + i2);
      assertEquals((range - 1) * (range / 2), result.intValue());
   }

   public void testObjIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Iterator<Map.Entry<Integer, String>> iterator = createStream(entrySet).iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> { assertEquals(cache.get(e.getKey()), e.getValue()); count.addAndGet(e.getKey());});
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testObjSortedIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Iterator<Map.Entry<Integer, String>> iterator = createStream(entrySet).sorted(
              (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).iterator();
      AtomicInteger i = new AtomicInteger();
      iterator.forEachRemaining(e -> {
         assertEquals(i.getAndIncrement(), e.getKey().intValue());
         assertEquals(cache.get(e.getKey()), e.getValue());
      });
   }

   public void testObjMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Iterator<String> iterator = createStream(entrySet).map(
              (Serializable & Function<Map.Entry<Integer, String>, String>) e -> e.getValue()).iterator();
      Set<String> set = new HashSet<>(range);
      iterator.forEachRemaining(set::add);
      assertEquals(range, set.size());
      IntStream.range(0, range).forEach(i -> assertTrue(set.contains(i + "-value")));
   }

   public void testObjFlatMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Iterator<String> iterator = createStream(entrySet).flatMap(
              (Serializable & Function<Map.Entry<Integer, String>, Stream<String>>)
                      e -> Arrays.stream(e.getValue().split("a"))).iterator();
      List<String> list = new ArrayList<>(range * 2);
      iterator.forEachRemaining(list::add);
      assertEquals(range * 2, list.size());
   }

   public void testObjToArray1() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Object[] array = createStream(entrySet).toArray();
      assertEquals(cache.size(), array.length);
      Spliterator<Map.Entry<Integer,String>> spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT |
              Spliterator.DISTINCT);
      StreamSupport.stream(spliterator, false).forEach(e -> assertEquals(cache.get(e.getKey()), e.getValue()));
   }

   public void testObjToArray2() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Map.Entry<Integer, String>[] array = createStream(entrySet).toArray(
              (Serializable & IntFunction<Map.Entry<Integer, String>[]>) Map.Entry[]::new);
      assertEquals(cache.size(), array.length);
      Spliterator<Map.Entry<Integer,String>> spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT |
              Spliterator.DISTINCT);
      StreamSupport.stream(spliterator, false).forEach(e -> assertEquals(cache.get(e.getKey()), e.getValue()));
   }

   // IntStream tests

   static final ToIntFunction<Map.Entry<Integer, String>> toInt =
           (Serializable & ToIntFunction<Map.Entry<Integer, String>>) e -> e.getKey();

   public void testIntAllMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).mapToInt(toInt).allMatch(
              (Serializable & IntPredicate) i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToInt(toInt).allMatch(
              (Serializable & IntPredicate) i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToInt(toInt).allMatch(
              (Serializable & IntPredicate) i -> i < 12));
   }

   public void testIntAnyMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToInt(toInt).anyMatch(
              (Serializable & IntPredicate) i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToInt(toInt).anyMatch(
              (Serializable & IntPredicate) i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToInt(toInt).anyMatch(
              (Serializable & IntPredicate) i -> i < 12));
   }

   public void testIntAverage() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).mapToInt(toInt).average().getAsDouble());
   }

   public void testIntCollect() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      HashSet<Integer> set = createStream(entrySet).mapToInt(toInt).collect(
              (Serializable & Supplier<HashSet<Integer>>) HashSet::new,
              (Serializable & ObjIntConsumer<HashSet<Integer>>) (s, i) -> s.add(i),
              (Serializable & BiConsumer<HashSet<Integer>, HashSet<Integer>>) (s1, s2) -> s1.addAll(s2));
      assertEquals(10, set.size());

   }

   public void testIntCount() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(10, createStream(entrySet).mapToInt(toInt).count());
   }

   public void testIntFindAny() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToInt(toInt).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToInt(toInt).filter((Serializable & IntPredicate) e -> e % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToInt(toInt).filter((Serializable & IntPredicate) e -> e < 10 && e >= 0).findAny().isPresent());
      assertFalse(createStream(entrySet).mapToInt(toInt).filter((Serializable & IntPredicate) e -> e > 12).findAny().isPresent());
   }

   public void testIntFindFirst() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToInt(toInt).sorted().findFirst().getAsInt());
   }

   public void testIntForEach() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         createStream(entrySet).mapToInt(toInt).forEach(
                 (Serializable & IntConsumer) e -> {
                    AtomicInteger atomic = getForEachObject(offset);
                    atomic.addAndGet(e);
                 });
         AtomicInteger atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2), atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testIntFlatMapForEach() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToInt(toInt).flatMap((Serializable & IntFunction<IntStream>)
                 i -> IntStream.of(i, 2)).forEach((Serializable & IntConsumer) e -> {
            AtomicInteger atomic = getForEachObject(offset);
            atomic.addAndGet(e);
         });
         AtomicInteger atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2) + 2 * range, atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testIntForEachOrdered() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Integer> list = new ArrayList<>(range);
      // we sort inverted order
      createStream(entrySet).mapToInt(toInt).sorted().forEachOrdered(
              list::add);
      assertEquals(range, list.size());
      for (int i = 0; i < range; ++i) {
         // 0 based so we have to also subtract by 1
         assertEquals(i, list.get(i).intValue());
      }
   }

   public void testIntIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfInt iterator = createStream(entrySet).mapToInt(toInt).iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining((int e) -> { assertTrue(cache.containsKey(e)); count.addAndGet(e); });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testIntSortedIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfInt iterator = createStream(entrySet).mapToInt(toInt).sorted().iterator();
      AtomicLong i = new AtomicLong();
      iterator.forEachRemaining((int e) -> assertEquals(i.getAndIncrement(), e));
   }

   public void testIntFlatMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfInt iterator = createStream(entrySet).flatMapToInt(
              (Serializable & Function<Map.Entry<Integer, String>, IntStream>)
                      e -> IntStream.of(e.getKey(), e.getValue().length())).iterator();

      int[] results = new int[range * 2];
      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         int next = iterator.nextInt();
         results[pos++] = next;
         if (next == 7) {
            halfCount++;
         }
         assertTrue(cache.containsKey(next));
      }
      assertEquals(range + 1, halfCount);
      assertEquals(range * 2, pos);
   }

   public void testIntNoneMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      assertFalse(createStream(entrySet).mapToInt(toInt).noneMatch(
              (Serializable & IntPredicate) i -> i % 2 == 0));
      assertTrue(createStream(entrySet).mapToInt(toInt).noneMatch(
              (Serializable & IntPredicate) i -> i > 10 && i < 0));
      assertFalse(createStream(entrySet).mapToInt(toInt).noneMatch(
              (Serializable & IntPredicate) i -> i < 12));
   }

   public void testIntReduce1() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToInt(toInt).reduce(1,
              (Serializable & IntBinaryOperator) (i1, i2) -> i1 * i2));

      assertEquals(362880, createStream(entrySet).mapToInt(toInt).filter(
              (Serializable & IntPredicate) i -> i != 0).reduce(1,
              (Serializable & IntBinaryOperator) (i1, i2) -> i1 * i2));
   }

   public void testIntReduce2() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToInt(toInt).reduce(
              (Serializable & IntBinaryOperator) (i1, i2) -> i1 * i2).getAsInt());

      assertEquals(362880, createStream(entrySet).mapToInt(toInt).filter(
              (Serializable & IntPredicate) i -> i != 0).reduce(
              (Serializable & IntBinaryOperator) (i1, i2) -> i1 * i2).getAsInt());
   }

   public void testIntSummaryStatistics() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      IntSummaryStatistics statistics = createStream(entrySet).mapToInt(toInt).summaryStatistics();
      assertEquals(0, statistics.getMin());
      assertEquals(9, statistics.getMax());
      assertEquals(4.5, statistics.getAverage());
      assertEquals((range - 1) * (range / 2), statistics.getSum());
      assertEquals(10, statistics.getCount());
   }

   public void testIntToArray() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      int[] array = createStream(entrySet).mapToInt(toInt).toArray();
      assertEquals(cache.size(), array.length);
      Spliterator.OfInt spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT);
      StreamSupport.intStream(spliterator, true).forEach(e -> assertTrue(cache.containsKey(e)));
   }

   public void testIntSum() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      int result = createStream(entrySet).mapToInt(toInt).sum();
      assertEquals((range - 1) * (range / 2), result);
   }

   public void testIntMax() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(9, createStream(entrySet).mapToInt(toInt).max().getAsInt());
   }

   public void testIntMin() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToInt(toInt).min().getAsInt());
   }

   // LongStream tests

   static final ToLongFunction<Map.Entry<Long, String>> toLong =
           (Serializable & ToLongFunction<Map.Entry<Long, String>>) e -> e.getKey();

   public void testLongAllMatch() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).mapToLong(toLong).allMatch(
              (Serializable & LongPredicate) i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToLong(toLong).allMatch(
              (Serializable & LongPredicate) i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToLong(toLong).allMatch(
              (Serializable & LongPredicate) i -> i < 12));
   }

   public void testLongAnyMatch() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToLong(toLong).anyMatch(
              (Serializable & LongPredicate) i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToLong(toLong).anyMatch(
              (Serializable & LongPredicate) i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToLong(toLong).anyMatch(
              (Serializable & LongPredicate) i -> i < 12));
   }

   public void testLongAverage() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).mapToLong(toLong).average().getAsDouble());
   }

   public void testLongCollect() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      HashSet<Long> set = createStream(entrySet).mapToLong(toLong).collect(
              (Serializable & Supplier<HashSet<Long>>) HashSet::new,
              (Serializable & ObjLongConsumer<HashSet<Long>>) (s, i) -> s.add(i),
              (Serializable & BiConsumer<HashSet<Long>, HashSet<Long>>) (s1, s2) -> s1.addAll(s2));
      assertEquals(10, set.size());
   }

   public void testLongCount() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(10, createStream(entrySet).mapToLong(toLong).count());
   }

   public void testLongFindAny() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToLong(toLong).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToLong(toLong).filter((Serializable & LongPredicate) e -> e % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToLong(toLong).filter((Serializable & LongPredicate) e -> e < 10 && e >= 0).findAny().isPresent());
      assertFalse(createStream(entrySet).mapToLong(toLong).filter((Serializable & LongPredicate) e -> e > 12).findAny().isPresent());
   }

   public void testLongFindFirst() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToLong(toLong).sorted().findFirst().getAsLong());
   }

   public void testLongForEach() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicLong());
      try {
         createStream(entrySet).mapToLong(toLong).forEach(
                 (Serializable & LongConsumer) e -> {
                    AtomicLong atomic = getForEachObject(offset);
                    atomic.addAndGet(e);
                 });
         AtomicLong atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2), atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testLongFlatMapForEach() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicLong());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToLong(toLong).flatMap((Serializable & LongFunction<LongStream>)
                 i -> LongStream.of(i, 2))
                 .forEach((Serializable & LongConsumer) e -> {
                    AtomicLong atomic = getForEachObject(offset);
                    atomic.addAndGet(e);
                 });
         AtomicLong atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2) + 2 * range, atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testLongForEachOrdered() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      List<Long> list = new ArrayList<>(range);
      // we sort inverted order
      createStream(entrySet).mapToLong(toLong).sorted().forEachOrdered(
              list::add);
      assertEquals(range, list.size());
      for (int i = 0; i < range; ++i) {
         // 0 based so we have to also subtract by 1
         assertEquals(i, list.get(i).longValue());
      }
   }

   public void testLongIterator() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfLong iterator = createStream(entrySet).mapToLong(toLong).iterator();
      AtomicLong count = new AtomicLong();
      iterator.forEachRemaining((long e) -> { assertTrue(cache.containsKey(e)); count.addAndGet(e); });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testLongSortedIterator() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfLong iterator = createStream(entrySet).mapToLong(toLong).sorted().iterator();
      AtomicLong i = new AtomicLong();
      iterator.forEachRemaining((long e) -> assertEquals(i.getAndIncrement(), e));
   }

   public void testLongFlatMapIterator() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfLong iterator = createStream(entrySet).flatMapToLong(
              (Serializable & Function<Map.Entry<Long, String>, LongStream>)
                      e -> LongStream.of(e.getKey(), e.getValue().length())).iterator();

      long[] results = new long[range * 2];
      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         long next = iterator.nextLong();
         results[pos++] = next;
         if (next == 7) {
            halfCount++;
         }
         assertTrue(cache.containsKey(next));
      }
      assertEquals(range + 1, halfCount);
      assertEquals(range * 2, pos);
   }

   public void testLongNoneMatch() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      assertFalse(createStream(entrySet).mapToLong(toLong).noneMatch(
              (Serializable & LongPredicate) i -> i % 2 == 0));
      assertTrue(createStream(entrySet).mapToLong(toLong).noneMatch(
              (Serializable & LongPredicate) i -> i > 10 && i < 0));
      assertFalse(createStream(entrySet).mapToLong(toLong).noneMatch(
              (Serializable & LongPredicate) i -> i < 12));
   }

   public void testLongReduce1() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToLong(toLong).reduce(1,
              (Serializable & LongBinaryOperator) (i1, i2) -> i1 * i2));

      assertEquals(362880, createStream(entrySet).mapToLong(toLong).filter(
              (Serializable & LongPredicate) i -> i != 0).reduce(1,
              (Serializable & LongBinaryOperator) (i1, i2) -> i1 * i2));
   }

   public void testLongReduce2() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToLong(toLong).reduce(
              (Serializable & LongBinaryOperator) (i1, i2) -> i1 * i2).getAsLong());

      assertEquals(362880, createStream(entrySet).mapToLong(toLong).filter(
              (Serializable & LongPredicate) i -> i != 0).reduce(
              (Serializable & LongBinaryOperator) (i1, i2) -> i1 * i2).getAsLong());
   }

   public void testLongSummaryStatistics() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      LongSummaryStatistics statistics = createStream(entrySet).mapToLong(toLong).summaryStatistics();
      assertEquals(0, statistics.getMin());
      assertEquals(9, statistics.getMax());
      assertEquals(4.5, statistics.getAverage());
      assertEquals((range - 1) * (range / 2), statistics.getSum());
      assertEquals(10, statistics.getCount());
   }

   public void testLongToArray() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      long[] array = createStream(entrySet).mapToLong(toLong).toArray();
      assertEquals(cache.size(), array.length);
      Spliterator.OfLong spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT);
      StreamSupport.longStream(spliterator, true).forEach(e -> assertTrue(cache.containsKey(e)));
   }

   public void testLongSum() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      long result = createStream(entrySet).mapToLong(toLong).sum();
      assertEquals((range - 1) * (range / 2), result);
   }

   public void testLongMax() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(9, createStream(entrySet).mapToLong(toLong).max().getAsLong());
   }

   public void testLongMin() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToLong(toLong).min().getAsLong());
   }

   // DoubleStream tests

   static final ToDoubleFunction<Map.Entry<Double, String>> toDouble =
           (Serializable & ToDoubleFunction<Map.Entry<Double, String>>) e -> e.getKey();

   public void testDoubleAllMatch() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).mapToDouble(toDouble).allMatch(
              (Serializable & DoublePredicate) i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).allMatch(
              (Serializable & DoublePredicate) i -> i > 5 && i < 0));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).allMatch(
              (Serializable & DoublePredicate) i -> i < 5));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).allMatch(
              (Serializable & DoublePredicate) i -> Math.floor(i) == i));
   }

   public void testDoubleAnyMatch() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToDouble(toDouble).anyMatch(
              (Serializable & DoublePredicate) i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).anyMatch(
              (Serializable & DoublePredicate) i -> i > 5 && i < 0));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).anyMatch(
              (Serializable & DoublePredicate) i -> i < 5));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).anyMatch(
              (Serializable & DoublePredicate) i -> Math.floor(i) == i));
   }

   public void testDoubleAverage() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(2.25, createStream(entrySet).mapToDouble(toDouble).average().getAsDouble());
   }

   public void testDoubleCollect() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      HashSet<Double> set = createStream(entrySet).mapToDouble(toDouble).collect(
              (Serializable & Supplier<HashSet<Double>>) HashSet::new,
              (Serializable & ObjDoubleConsumer<HashSet<Double>>) (s, i) -> s.add(i),
              (Serializable & BiConsumer<HashSet<Double>, HashSet<Double>>) (s1, s2) -> s1.addAll(s2));
      assertEquals(10, set.size());
   }

   public void testDoubleCount() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(10, createStream(entrySet).mapToDouble(toDouble).count());
   }

   public void testDoubleFindAny() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToDouble(toDouble).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToDouble(toDouble).filter((Serializable & DoublePredicate) e -> e % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToDouble(toDouble).filter((Serializable & DoublePredicate) e -> e < 5 && e >= 0).findAny().isPresent());
      assertFalse(createStream(entrySet).mapToDouble(toDouble).filter((Serializable & DoublePredicate) e -> e > 5).findAny().isPresent());
   }

   public void testDoubleFindFirst() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).sorted().findFirst().getAsDouble());
   }

   public void testDoubleForEach() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());
      try {
         createStream(entrySet).mapToDouble(toDouble).forEach(
                 (Serializable & DoubleConsumer) e -> {
                    DoubleSummaryStatistics stats = getForEachObject(offset);
                    synchronized (stats) {
                       stats.accept(e);
                    }
                 });
         DoubleSummaryStatistics stats = getForEachObject(offset);
         assertEquals(2.25, stats.getAverage());
         assertEquals(0.0, stats.getMin());
         assertEquals(4.5, stats.getMax());
         assertEquals(10, stats.getCount());
         assertEquals(22.5, stats.getSum());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testDoubleFlatMapForEach() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToDouble(toDouble).flatMap(
                 (Serializable & DoubleFunction<DoubleStream>)
                         e -> DoubleStream.of(e, 2.25)).forEach(
                 (Serializable & DoubleConsumer) e -> {
                    DoubleSummaryStatistics stats = getForEachObject(offset);
                    synchronized (stats) {
                       stats.accept(e);
                    }
                 });
         DoubleSummaryStatistics stats = getForEachObject(offset);
         assertEquals(2.25, stats.getAverage());
         assertEquals(0.0, stats.getMin());
         assertEquals(4.5, stats.getMax());
         assertEquals(20, stats.getCount());
         assertEquals(45.0, stats.getSum());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testDoubleForEachOrdered() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      List<Double> list = new ArrayList<>(range);
      // we sort inverted order
      createStream(entrySet).mapToDouble(toDouble).sorted().forEachOrdered(
              list::add);
      assertEquals(range, list.size());
      for (int i = 0; i < range; ++i) {
         // 0 based so we have to also subtract by 1
         assertEquals((double) i / 2, list.get(i));
      }
   }

   public void testDoubleIterator() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfDouble iterator = createStream(entrySet).mapToDouble(toDouble).iterator();
      DoubleSummaryStatistics doubleSummaryStatistics = new DoubleSummaryStatistics();
      iterator.forEachRemaining((double e) -> { assertTrue(cache.containsKey(e)); doubleSummaryStatistics.accept(e); });
      assertEquals(2.25, doubleSummaryStatistics.getAverage());
      assertEquals(0.0, doubleSummaryStatistics.getMin());
      assertEquals(4.5, doubleSummaryStatistics.getMax());
      assertEquals(10, doubleSummaryStatistics.getCount());
      assertEquals(22.5, doubleSummaryStatistics.getSum());
   }

   public void testDoubleSortedIterator() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfDouble iterator = createStream(entrySet).mapToDouble(toDouble).sorted().iterator();
      AtomicInteger i = new AtomicInteger();
      iterator.forEachRemaining((double e) -> assertEquals((double) i.getAndIncrement() / 2, e));
   }

   public void testDoubleFlatMapIterator() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfDouble iterator = createStream(entrySet).flatMapToDouble(
              (Serializable & Function<Map.Entry<Double, String>, DoubleStream>)
                      e -> DoubleStream.of(e.getKey(), .5)).iterator();

      double[] results = new double[range * 2];
      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         double next = iterator.nextDouble();
         results[pos++] = next;
         if (next == 0.5) {
            halfCount++;
         }
         assertTrue(cache.containsKey(next));
      }
      assertEquals(range + 1, halfCount);
      assertEquals(range * 2, pos);
   }

   public void testDoubleNoneMatch() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      assertFalse(createStream(entrySet).mapToDouble(toDouble).noneMatch(
              (Serializable & DoublePredicate) i -> i % 2 == 0));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).noneMatch(
              (Serializable & DoublePredicate) i -> i > 5 && i < 0));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).noneMatch(
              (Serializable & DoublePredicate) i -> i < 5));
   }

   public void testDoubleReduce1() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // One value is 0.0 so multiplying them together should be 0.0
      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).reduce(1.0,
              (Serializable & DoubleBinaryOperator) (i1, i2) -> i1 * i2));

      assertEquals(708.75, createStream(entrySet).mapToDouble(toDouble).filter(
              (Serializable & DoublePredicate) i -> i != 0).reduce(1.0,
              (Serializable & DoubleBinaryOperator) (i1, i2) -> i1 * i2));
   }

   public void testDoubleReduce2() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // One value is 0.0 so multiplying them together should be 0.0
      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).reduce(
              (Serializable & DoubleBinaryOperator) (i1, i2) -> i1 * i2).getAsDouble());

      assertEquals(708.75, createStream(entrySet).mapToDouble(toDouble).filter(
              (Serializable & DoublePredicate) i -> i != 0).reduce(
              (Serializable & DoubleBinaryOperator) (i1, i2) -> i1 * i2).getAsDouble());
   }

   public void testDoubleSummaryStatistics() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      DoubleSummaryStatistics statistics = createStream(entrySet).mapToDouble(toDouble).summaryStatistics();
      assertEquals(2.25, statistics.getAverage());
      assertEquals(0.0, statistics.getMin());
      assertEquals(4.5, statistics.getMax());
      assertEquals(10, statistics.getCount());
      assertEquals(22.5, statistics.getSum());
   }

   public void testDoubleToArray() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      double[] array = createStream(entrySet).mapToDouble(toDouble).toArray();
      assertEquals(cache.size(), array.length);
      Spliterator.OfDouble spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT);
      StreamSupport.doubleStream(spliterator, true).forEach(e -> assertTrue(cache.containsKey(e)));
   }

   public void testDoubleSum() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      double result = createStream(entrySet).mapToDouble(toDouble).sum();
      assertEquals((double) (range - 1) * (range / 2) / 2, result);
   }

   public void testDoubleMax() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).mapToDouble(toDouble).max().getAsDouble());
   }

   public void testDoubleMin() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).min().getAsDouble());
   }

   // KeySet Tests
   public void testObjKeySetMax() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Integer> keySet = cache.keySet();

      assertEquals(9, createStream(keySet).max((Serializable & Comparator<Integer>)
                      (e1, e2) -> Integer.compare(e1, e2)).get().intValue());
   }

   public void testKeySetIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Integer> keySet = cache.keySet();

      Iterator<Integer> iterator = createStream(keySet).iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> { assertTrue(cache.containsKey(e)); count.addAndGet(e);});
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testKeySetMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Integer> keySet = cache.keySet();

      Iterator<String> iterator = createStream(keySet).map(
              (Serializable & Function<Integer, String>) i -> i + "-value").iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> {
         Integer key = Integer.valueOf(e.substring(0, 1));
         assertEquals(cache.get(key), e); count.addAndGet(key);
      });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testKeySetFlatMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheSet<Integer> keySet = cache.keySet();

      PrimitiveIterator.OfInt iterator = createStream(keySet).flatMapToInt(
              (Serializable & Function<Integer, IntStream>)
              i -> IntStream.of(i, 3)).iterator();
      int[] results = new int[range * 2];
      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         int next = iterator.nextInt();
         results[pos++] = next;
         if (next == 3) {
            halfCount++;
         }
         assertTrue(cache.containsKey(next));
      }
      assertEquals(range + 1, halfCount);
      assertEquals(range * 2, pos);
   }

   // Values Tests
   public void testObjValuesMax() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheCollection<String> keySet = cache.values();

      assertEquals("9-value",
              createStream(keySet).max((Serializable & Comparator<String>)
                      (e1, e2) -> Integer.compare(
                              Integer.valueOf(e1.substring(0, 1)),
                              Integer.valueOf(e2.substring(0, 1)))).get());
   }

   public void testObjValuesIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheCollection<String> values = cache.values();

      Iterator<String> iterator = createStream(values).iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> {
         Integer key = Integer.valueOf(e.substring(0, 1));
         assertEquals(cache.get(key), e); count.addAndGet(key);
      });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testValuesMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheCollection<String> values = cache.values();

      PrimitiveIterator.OfInt iterator = createStream(values).mapToInt(
              (Serializable & ToIntFunction<String>)
                      e -> Integer.valueOf(e.substring(0, 1))).iterator();

      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining((int e) -> {
         assertTrue(cache.containsKey(e));
         count.addAndGet(e);
      });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testValuesFlatMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(range, cache.size());
      CacheCollection<String> values = cache.values();

      PrimitiveIterator.OfInt iterator = createStream(values).flatMapToInt(
              (Serializable & Function<String, IntStream>)
                      e -> IntStream.of(Integer.valueOf(e.substring(0, 1)), e.length())).iterator();

      int[] results = new int[range * 2];
      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         int next = iterator.nextInt();
         results[pos++] = next;
         if (next == 7) {
            halfCount++;
         }
         assertTrue(cache.containsKey(next));
      }
      assertEquals(range + 1, halfCount);
      assertEquals(range * 2, pos);
   }
}
