package org.infinispan.stream;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.GlobalContextInitializer;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.protostream.types.java.arrays.AbstractArrayAdapter;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.function.SerializableToDoubleFunction;
import org.infinispan.util.function.SerializableToIntFunction;
import org.infinispan.util.function.SerializableToLongFunction;
import org.testng.annotations.Test;

/**
 * Base test class for streams to verify proper behavior of all of the terminal operations for all of the various
 * stream classes
 */
@Test(groups = "functional")
public abstract class BaseStreamTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = "testCache";
   protected ConfigurationBuilder builderUsed;

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


   public BaseStreamTest(boolean tx) {
      this.transactional = tx;
   }

   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      // Do nothing to config by default, used by people who extend this
   }

   protected abstract <E> CacheStream<E> createStream(CacheCollection<E> cacheCollection);

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (transactional) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().stateTransfer().chunkSize(50);
         enhanceConfiguration(builderUsed);
         createClusteredCaches(3, CACHE_NAME, BaseStreamTestSCI.INSTANCE, builderUsed);
      } else {
         enhanceConfiguration(builderUsed);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builderUsed);
         cacheManagers.add(cm);
         cm.defineConfiguration(CACHE_NAME, builderUsed.build());
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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).allMatch(e -> e.getValue().endsWith("-value")));
      assertFalse(createStream(entrySet).allMatch(e -> e.getKey() % 2 == 0));
      assertTrue(createStream(entrySet).allMatch(e -> e.getKey() < 10 && e.getKey() >= 0));
      assertTrue(createStream(entrySet).allMatch(e -> e.getKey().toString().equals(e.getValue().substring(0, 1))));
   }

   public void testObjAnyMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).anyMatch(e -> e.getValue().endsWith("-value")));
      assertTrue(createStream(entrySet).anyMatch(e -> e.getKey() % 2 == 0));
      assertTrue(createStream(entrySet).anyMatch(e -> e.getKey() < 10 && e.getKey() >= 0));
      assertTrue(createStream(entrySet).anyMatch(e -> e.getValue().equals("4-value")));
      assertFalse(createStream(entrySet).anyMatch(e -> e.getKey() > 12));
   }

   public void testObjCollectorIntAverager() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).collect(
            () -> Collectors.averagingInt(Map.Entry::getKey)));
   }

   public void testObjCollectorIntStatistics() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      IntSummaryStatistics stats = createStream(entrySet).collect(
            () -> Collectors.summarizingInt(Map.Entry::getKey));
      assertEquals(10, stats.getCount());
      assertEquals(4.5, stats.getAverage());
      assertEquals(0, stats.getMin());
      assertEquals(9, stats.getMax());
      assertEquals(45, stats.getSum());
   }

   public void testObjCollectorGroupBy() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      ConcurrentMap<Boolean, List<Map.Entry<Integer, String>>> grouped = createStream(entrySet).collect(
                  () -> Collectors.groupingByConcurrent(k -> k.getKey() % 2 == 0));
      grouped.get(true).parallelStream().forEach(e -> assertTrue(e.getKey() % 2 == 0));
      grouped.get(false).parallelStream().forEach(e -> assertTrue(e.getKey() % 2 == 1));
   }

   public void testObjCollect() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Map.Entry<Integer, String>> list = createStream(entrySet).collect(ArrayList::new,
            ArrayList::add, ArrayList::addAll);
      assertEquals(cache.size(), list.size());
      list.parallelStream().forEach(e -> assertEquals(cache.get(e.getKey()), e.getValue()));
   }

   public void testObjSortedList() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Map.Entry<Integer, String>> list = createStream(entrySet).sorted(
            (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).toList();
      assertEquals(cache.size(), list.size());
      AtomicInteger i = new AtomicInteger();
      list.forEach(e -> {
         assertEquals(i.getAndIncrement(), e.getKey().intValue());
         assertEquals(cache.get(e.getKey()), e.getValue());
      });
   }

   public void testObjSortedCollector() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      List<Map.Entry<Integer, String>> list = createStream(entrySet).sorted(
            (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).collect(
            Collectors::<Map.Entry<Integer, String>>toList);
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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(range, createStream(entrySet).count());
   }

   public void testObjFindAny() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).findAny().isPresent());
      assertTrue(createStream(entrySet).filter(e -> e.getValue().endsWith("-value")).findAny().isPresent());
      assertTrue(createStream(entrySet).filter(e -> e.getKey() % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).filter(e -> e.getKey() < 10 && e.getKey() >= 0).findAny().isPresent());
      assertTrue(createStream(entrySet).filter(e -> e.getValue().equals("4-value")).findAny().isPresent());
      assertFalse(createStream(entrySet).filter(e -> e.getKey() > 12).findAny().isPresent());
   }

   public void testObjFindFirst() {
      Cache<Integer, String> cache = getCache(0);
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).findFirst().isEmpty());

      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      assertEquals(0, createStream(entrySet).sorted(
            (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).findFirst().get().getKey().intValue());
   }

   public void testObjForEach() {
      Cache<Integer, String> cache = getCache(0);

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.entrySet()).forEach(e -> {
               AtomicInteger atomic = getForEachObject(offset);
               atomic.addAndGet(e.getKey());

            });

            return ((AtomicInteger) getForEachObject(offset)).get();
         }, cache);
      } finally {
         clearForEachObject(offset);
      }
   }

   private static abstract class AbstractForEach implements CacheAware<Integer, String> {
      Cache<?, ?> cache;

      @ProtoField(1)
      final int cacheOffset;

      @ProtoField(2)
      final int atomicOffset;

      AbstractForEach(int cacheOffset, int atomicOffset) {
         this.cacheOffset = cacheOffset;
         this.atomicOffset = atomicOffset;
      }

      public void injectCache(Cache<Integer, String> cache) {
         this.cache = cache;
      }
   }

   public static class ForEachInjected<E> extends AbstractForEach implements Consumer<E> {
      private final ToIntFunction<? super E> function;

      private ForEachInjected(int cacheOffset, int atomicOffset, SerializableToIntFunction<? super E> function) {
         super(cacheOffset, atomicOffset);
         this.function = function;
      }

      @ProtoFactory
      ForEachInjected(int cacheOffset, int atomicOffset, MarshallableObject<ToIntFunction<? super E>> function) {
         this(cacheOffset, atomicOffset, (SerializableToIntFunction<? super E>) MarshallableObject.unwrap(function));
      }

      @ProtoField(3)
      MarshallableObject<ToIntFunction<? super E>> getFunction() {
         return MarshallableObject.create(function);
      }

      @Override
      public void accept(E entry) {
         Cache<?, ?> cache = getForEachObject(cacheOffset);
         if (cache != null && this.cache != null && cache.getName().equals(this.cache.getName())) {
            ((AtomicInteger) getForEachObject(atomicOffset)).addAndGet(function.applyAsInt(entry));
         } else {
            fail("Did not receive correct cache!");
         }
      }
   }

   public void testObjForEachCacheInjected() {
      Cache<Integer, String> cache = getCache(0);

      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.entrySet()).forEach(new ForEachInjected<>(cacheOffset, atomicOffset, Map.Entry::getKey));
            return ((AtomicInteger) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testObjForEachBiConsumer() {
      Cache<Integer, String> cache = getCache(0);

      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.entrySet()).forEach((c, e) -> {
               Cache<?, ?> localCache = getForEachObject(cacheOffset);
               if (c != null && localCache != null && c.getName().equals(localCache.getName())) {
                  ((AtomicInteger) getForEachObject(atomicOffset)).addAndGet(e.getKey());
               } else {
                  fail("Did not receive correct cache!");
               }
            });
            return ((AtomicInteger) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testObjKeySetForEachCacheInjected() {
      Cache<Integer, String> cache = getCache(0);

      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.keySet()).forEach(new ForEachInjected<>(cacheOffset, atomicOffset, Integer::intValue));
            return ((AtomicInteger) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testObjValuesForEachCacheInjected() {
      Cache<Integer, String> cache = getCache(0);

      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.values()).forEach(new ForEachInjected<>(cacheOffset, atomicOffset,
                  e -> Integer.valueOf(e.substring(0, 1))));
            return ((AtomicInteger) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testObjFlatMapForEach() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Queue<String> queue = new ConcurrentLinkedQueue<>();
      int queueOffset = populateNextForEachStructure(queue);
      try {
         createStream(entrySet).distributedBatchSize(5)
               .flatMap(e -> Arrays.stream(e.getValue().split("a")))
               .forEach(e -> {
                  Queue<String> localQueue = getForEachObject(queueOffset);
                  localQueue.add(e);
               });
         assertEquals(range * 2, queue.size());

         int lueCount = 0;
         for (String string : queue) {
            if (string.equals("lue")) lueCount++;
         }
         assertEquals(10, lueCount);
      } finally {
         clearForEachObject(queueOffset);
      }
   }

   public void testObjForEachOrdered() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(Integer.valueOf(9),
            createStream(entrySet).max((e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).get().getKey());
   }

   public void testObjMin() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(Integer.valueOf(0),
            createStream(entrySet).min((e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).get().getKey());
   }

   public void testObjNoneMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).noneMatch(e -> e.getValue().endsWith("-value")));
      assertFalse(createStream(entrySet).noneMatch(e -> e.getKey() % 2 == 0));
      assertFalse(createStream(entrySet).noneMatch(e -> e.getKey() < 10 && e.getKey() >= 0));
      assertFalse(createStream(entrySet).noneMatch(e -> e.getKey().toString().equals(e.getValue().substring(0, 1))));
      assertTrue(createStream(entrySet).noneMatch(e -> e.getKey() > 12));
   }

   public void testObjReduce1() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Optional<Map.Entry<Integer, String>> optional = createStream(entrySet).reduce(
            (e1, e2) -> new ImmortalCacheEntry(e1.getKey() + e2.getKey(), e1.getValue() + e2.getValue()));
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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Map.Entry<Integer, String> result = createStream(entrySet).reduce(new ImmortalCacheEntry(0, ""),
            (e1, e2) -> new ImmortalCacheEntry(e1.getKey() + e2.getKey(), e1.getValue() + e2.getValue()));
      assertEquals((range - 1) * (range / 2), result.getKey().intValue());
      assertEquals(range * 7, result.getValue().length());
   }

   public void testObjReduce2WithMap() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Integer result = createStream(entrySet).map(Map.Entry::getKey).reduce(0, (e1, e2) -> e1 + e2);
      assertEquals((range - 1) * (range / 2), result.intValue());
   }

   public void testObjReduce3() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      Integer result = createStream(entrySet).reduce(0, (e1, e2) -> e1 + e2.getKey(), (i1, i2) -> i1 + i2);
      assertEquals((range - 1) * (range / 2), result.intValue());
   }

   public void testObjIterator() {
      Cache<Integer, String> cache = getCache(0);

      testIntOperation(() -> {
         Iterator<Map.Entry<Integer, String>> iterator = createStream(cache.entrySet()).iterator();
         AtomicInteger count = new AtomicInteger();
         iterator.forEachRemaining(e -> {
            assertEquals(cache.get(e.getKey()), e.getValue());
            count.addAndGet(e.getKey());
         });
         return count.get();
      }, cache);
   }

   public void testObjSortedIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Iterator<String> iterator = createStream(entrySet).map(Map.Entry::getValue).iterator();
      Set<String> set = new HashSet<>(range);
      iterator.forEachRemaining(set::add);
      assertEquals(range, set.size());
      IntStream.range(0, range).forEach(i -> assertTrue(set.contains(i + "-value")));
   }

   public void testObjFlatMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      Map<Integer, IntSet> keysBySegment = log.isTraceEnabled() ? new TreeMap<>() : null;
      KeyPartitioner kp = TestingUtil.extractComponent(cache, KeyPartitioner.class);
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> {
         if (keysBySegment != null) {
            int segment = kp.getSegment(i);
            IntSet keys = keysBySegment.computeIfAbsent(segment, IntSets::mutableEmptySet);
            keys.set(i);
         }
         cache.put(i, i + "-value" + i);
      });

      if (keysBySegment != null) {
         log.tracef("Keys by segment are: " + keysBySegment);
      }

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      StringJoiner stringJoiner = new StringJoiner(" ");
      // Rxjava requets 128 by default for many operations - thus we have a number larger than that
      int explosionCount = 293;
      for (int i = 0; i < explosionCount; ++i) {
         stringJoiner.add("special-" + String.valueOf(i));
      }

      String specialString = stringJoiner.toString();

      Iterator<String> iterator = createStream(entrySet)
            .distributedBatchSize(1)
            .flatMap(e -> {
               if (e.getKey() == 2) {
                  // Make sure to test an empty stream as well
                  return Stream.empty();
               }
               if (e.getKey() == 5) {
                  // Make sure we also test a very large resulting stream without the key in it
                  return Arrays.stream(specialString.split(" "));
               }
               return Arrays.stream(e.getValue().split("a"));
            })
            .iterator();
      List<String> list = new ArrayList<>(range * 2);
      iterator.forEachRemaining(list::add);
      if (keysBySegment != null) {
         log.tracef("Returned values are: %s", list);
      }
      assertEquals((range - 2) * 2 + explosionCount, list.size());
   }

   public void testObjToArray1() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Object[] array = createStream(entrySet).toArray();
      assertEquals(cache.size(), array.length);
      Spliterator<Map.Entry<Integer, String>> spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT |
            Spliterator.NONNULL);
      StreamSupport.stream(spliterator, false).forEach(e -> assertEquals(cache.get(e.getKey()), e.getValue()));
   }

   public void testObjToArray2() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      KeyValuePair<Integer, String>[] array = createStream(entrySet).map(e -> new KeyValuePair<>(e.getKey(), e.getValue())).toArray(KeyValuePair[]::new);
      assertEquals(cache.size(), array.length);
      Spliterator<KeyValuePair<Integer, String>> spliterator = Spliterators.spliterator(array, Spliterator.DISTINCT |
            Spliterator.NONNULL);
      StreamSupport.stream(spliterator, false).forEach(e -> assertEquals(cache.get(e.getKey()), e.getValue()));
   }

   @ProtoAdapter(KeyValuePair[].class)
   @ProtoName("KeyValuePairArray")
   static class KeyValuePairArrayAdapter extends AbstractArrayAdapter<KeyValuePair> {
      @ProtoFactory
      public KeyValuePair[] create(int size) {
         return new KeyValuePair[size];
      }
   }

   public void testObjSortedSkipIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      for (int i = 0; i < range; ++i) {
         Iterator<Map.Entry<Integer, String>> iterator = createStream(entrySet).sorted(
               (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).skip(i).iterator();
         AtomicInteger atomicInteger = new AtomicInteger(i);
         iterator.forEachRemaining(e -> {
            assertEquals(atomicInteger.getAndIncrement(), e.getKey().intValue());
            assertEquals(cache.get(e.getKey()), e.getValue());
         });
         assertEquals(range, atomicInteger.get());
      }
   }

   public void testObjSortedLimitIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      for (int i = 1; i < range; ++i) {
         Iterator<Map.Entry<Integer, String>> iterator = createStream(entrySet).sorted(
               (e1, e2) -> Integer.compare(e1.getKey(), e2.getKey())).limit(i).iterator();
         AtomicInteger atomicInteger = new AtomicInteger();
         iterator.forEachRemaining(e -> {
            assertEquals(atomicInteger.getAndIncrement(), e.getKey().intValue());
            assertEquals(cache.get(e.getKey()), e.getValue());
         });
         assertEquals(i, atomicInteger.get());
      }
   }

   public void testObjPointlessSortMap() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      IntSummaryStatistics stats = createStream(entrySet).sorted((e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
            .mapToInt(Map.Entry::getKey).summaryStatistics();
      assertEquals(range, stats.getCount());
      assertEquals(0, stats.getMin());
      assertEquals(9, stats.getMax());
   }

   // IntStream tests

   static final SerializableToIntFunction<Map.Entry<Integer, String>> toInt = Map.Entry::getKey;


   public void testIntAllMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).mapToInt(toInt).allMatch(i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToInt(toInt).allMatch(i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToInt(toInt).allMatch(i -> i < 12));
   }

   public void testIntAnyMatch() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToInt(toInt).anyMatch(i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToInt(toInt).anyMatch(i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToInt(toInt).anyMatch(i -> i < 12));
   }

   public void testIntAverage() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).mapToInt(toInt).average().getAsDouble());
   }

   public void testIntCollect() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      HashSet<Integer> set = createStream(entrySet).mapToInt(toInt).collect(HashSet::new, Set::add, Set::addAll);
      assertEquals(10, set.size());

   }

   public void testIntCount() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(10, createStream(entrySet).mapToInt(toInt).count());
   }

   public void testIntFindAny() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToInt(toInt).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToInt(toInt).filter(e -> e % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToInt(toInt).filter(e -> e < 10 && e >= 0).findAny().isPresent());
      assertFalse(createStream(entrySet).mapToInt(toInt).filter(e -> e > 12).findAny().isPresent());
   }

   public void testIntFindFirst() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToInt(toInt).sorted().findFirst().getAsInt());
   }

   public void testIntForEach() {
      Cache<Integer, String> cache = getCache(0);

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.entrySet()).mapToInt(toInt).forEach(e -> {
               AtomicInteger atomic = getForEachObject(offset);
               atomic.addAndGet(e);
            });
            return ((AtomicInteger) getForEachObject(offset)).get();
         }, cache);
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testIntFlatMapForEach() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToInt(toInt).flatMap(i -> IntStream.of(i, 2))
               .forEach(e -> {

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

   public static class ForEachIntInjected extends AbstractForEach implements IntConsumer {

      @ProtoFactory
      ForEachIntInjected(int cacheOffset, int atomicOffset) {
         super(cacheOffset, atomicOffset);
      }

      @Override
      public void accept(int value) {
         Cache<?, ?> cache = getForEachObject(cacheOffset);
         if (cache != null && this.cache != null && cache.getName().equals(this.cache.getName())) {
            ((AtomicInteger) getForEachObject(atomicOffset)).addAndGet(value);
         } else {
            fail("Did not receive correct cache!");
         }
      }
   }

   private void testIntOperation(Supplier<Integer> intSupplier, Cache<Integer, String> cache) {
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));


      assertEquals((range - 1) * (range / 2), intSupplier.get().intValue());
   }

   public void testIntForEachCacheInjected() {
      Cache<Integer, String> cache = getCache(0);

      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicInteger());
      try {
         testIntOperation(() -> {
            createStream(cache.entrySet()).mapToInt(toInt).forEach(new ForEachIntInjected(cacheOffset, atomicOffset));
            return ((AtomicInteger) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testIntForEachBiConsumer() {
      Cache<Integer, String> cache = getCache(0);
      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicInteger());

      try {
         testIntOperation(() -> {
            createStream(cache.entrySet()).mapToInt(toInt).forEach((c, i) -> {
               Cache<?, ?> localCache = getForEachObject(cacheOffset);
               if (c != null && localCache != null && c.getName().equals(localCache.getName())) {
                  AtomicInteger atomicInteger = getForEachObject(atomicOffset);
                  atomicInteger.addAndGet(i);
               }
            });
            return ((AtomicInteger) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testIntFlatMapObjConsumerForEach() {
      Cache<Integer, String> cache = getCache(0);
      String cacheName = cache.getName();
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicInteger());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToInt(toInt).flatMap(i -> IntStream.of(i, 2))
               .forEach((c, e) -> {
                  assertEquals(cacheName, c.getName());
                  AtomicInteger atomic = getForEachObject(offset);
                  atomic.addAndGet(e);
               });
         AtomicInteger atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2) + 2 * range, atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testIntIterator() {
      Cache<Integer, String> cache = getCache(0);

      testIntOperation(() -> {
         PrimitiveIterator.OfInt iterator = createStream(cache.entrySet()).mapToInt(toInt).iterator();
         AtomicInteger count = new AtomicInteger();
         iterator.forEachRemaining((int e) -> {
            assertTrue(cache.containsKey(e));
            count.addAndGet(e);
         });
         return count.get();
      }, cache);
   }

   public void testIntSortedIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfInt iterator = createStream(entrySet).flatMapToInt(
            e -> IntStream.of(e.getKey(), e.getValue().length())).iterator();

      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         int next = iterator.nextInt();
         pos++;
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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      assertFalse(createStream(entrySet).mapToInt(toInt).noneMatch(i -> i % 2 == 0));
      assertTrue(createStream(entrySet).mapToInt(toInt).noneMatch(i -> i > 10 && i < 0));
      assertFalse(createStream(entrySet).mapToInt(toInt).noneMatch(i -> i < 12));
   }

   public void testIntReduce1() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToInt(toInt).reduce(1, (i1, i2) -> i1 * i2));

      assertEquals(362880, createStream(entrySet).mapToInt(toInt).filter(i -> i != 0).reduce(1, (i1, i2) -> i1 * i2));
   }

   public void testIntReduce2() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToInt(toInt).reduce((i1, i2) -> i1 * i2).getAsInt());

      assertEquals(362880, createStream(entrySet).mapToInt(toInt).filter(i -> i != 0).reduce((i1, i2) -> i1 * i2)
            .getAsInt());
   }

   public void testIntSummaryStatistics() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(9, createStream(entrySet).mapToInt(toInt).max().getAsInt());
   }

   public void testIntMin() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToInt(toInt).min().getAsInt());
   }

   public void testIntSortedSkip() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).forEach(i -> cache.put(i, i + "-value"));
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      for (int i = 0; i < range; i++) {
         IntSummaryStatistics stats = createStream(entrySet).mapToInt(toInt)
               .sorted().skip(i).summaryStatistics();
         assertEquals(range - i, stats.getCount());
         assertEquals(i, stats.getMin());
         assertEquals(range - 1, stats.getMax());
         assertEquals(IntStream.range(i, range).sum(), stats.getSum());
      }
   }

   public void testIntSortedLimit() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).forEach(i -> cache.put(i, i + "-value"));
      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      for (int i = 1; i < range; i++) {
         IntSummaryStatistics stats = createStream(entrySet).mapToInt(toInt)
               .sorted().limit(i).summaryStatistics();
         assertEquals(i, stats.getCount());
         assertEquals(0, stats.getMin());
         assertEquals(i - 1, stats.getMax());
         assertEquals(IntStream.range(0, i).sum(), stats.getSum());
      }
   }


   // LongStream tests

   static final SerializableToLongFunction<Map.Entry<Long, String>> toLong = Map.Entry::getKey;

   public void testLongAllMatch() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).mapToLong(toLong).allMatch(i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToLong(toLong).allMatch(i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToLong(toLong).allMatch(i -> i < 12));
   }

   public void testLongAnyMatch() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToLong(toLong).anyMatch(i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToLong(toLong).anyMatch(i -> i > 10 && i < 0));
      assertTrue(createStream(entrySet).mapToLong(toLong).anyMatch(i -> i < 12));
   }

   public void testLongAverage() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).mapToLong(toLong).average().getAsDouble());
   }

   public void testLongCollect() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      HashSet<Long> set = createStream(entrySet).mapToLong(toLong).collect(HashSet::new, Set::add, Set::addAll);
      assertEquals(10, set.size());
   }

   public void testLongCount() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(10, createStream(entrySet).mapToLong(toLong).count());
   }

   public void testLongFindAny() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToLong(toLong).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToLong(toLong).filter(e -> e % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToLong(toLong).filter(e -> e < 10 && e >= 0).findAny().isPresent());
      assertFalse(createStream(entrySet).mapToLong(toLong).filter(e -> e > 12).findAny().isPresent());
   }

   public void testLongFindFirst() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToLong(toLong).sorted().findFirst().getAsLong());
   }

   public void testLongForEach() {
      Cache<Long, String> cache = getCache(0);

      int offset = populateNextForEachStructure(new AtomicLong());
      try {
         testLongOperation(() -> {
            createStream(cache.entrySet()).mapToLong(toLong).forEach(e -> {

               AtomicLong atomic = getForEachObject(offset);
               atomic.addAndGet(e);
            });
            return ((AtomicLong) getForEachObject(offset)).get();
         }, cache);
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testLongFlatMapForEach() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicLong());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToLong(toLong).flatMap(i -> LongStream.of(i, 2))
               .forEach(e -> {
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

   public static class ForEachLongInjected extends AbstractForEach implements LongConsumer {

      @ProtoFactory
      ForEachLongInjected(int cacheOffset, int atomicOffset) {
         super(cacheOffset, atomicOffset);
      }

      @Override
      public void accept(long value) {
         Cache<?, ?> cache = getForEachObject(cacheOffset);
         if (cache != null && this.cache != null && cache.getName().equals(this.cache.getName())) {
            ((AtomicLong) getForEachObject(atomicOffset)).addAndGet(value);
         } else {
            fail("Did not receive correct cache!");
         }
      }
   }

   private void testLongOperation(Supplier<Long> longSupplier, Cache<Long, String> cache) {
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));


      assertEquals((range - 1) * (range / 2), longSupplier.get().longValue());
   }

   public void testLongForEachCacheInjected() {
      Cache<Long, String> cache = getCache(0);

      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicLong());
      try {
         testLongOperation(() -> {
            createStream(cache.entrySet()).mapToLong(toLong).forEach(new ForEachLongInjected(cacheOffset, atomicOffset));
            return ((AtomicLong) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testLongForEachBiConsumer() {
      Cache<Long, String> cache = getCache(0);
      int cacheOffset = populateNextForEachStructure(cache);
      int atomicOffset = populateNextForEachStructure(new AtomicLong());

      try {
         testLongOperation(() -> {
            createStream(cache.entrySet()).mapToLong(toLong).forEach((c, i) -> {
               Cache<?, ?> localCache = getForEachObject(cacheOffset);
               if (c != null && localCache != null && c.getName().equals(localCache.getName())) {
                  AtomicLong atomicLong = getForEachObject(atomicOffset);
                  atomicLong.addAndGet(i);
               }
            });
            return ((AtomicLong) getForEachObject(atomicOffset)).get();
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(atomicOffset);
      }
   }

   public void testLongFlatMapObjConsumerForEach() {
      Cache<Long, String> cache = getCache(0);
      String cacheName = cache.getName();
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new AtomicLong());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToLong(toLong).flatMap(i -> LongStream.of(i, 2))
               .forEach((c, e) -> {
                  assertEquals(cacheName, c.getName());
                  AtomicLong atomic = getForEachObject(offset);
                  atomic.addAndGet(e);
               });
         AtomicLong atomic = getForEachObject(offset);
         assertEquals((range - 1) * (range / 2) + 2 * range, atomic.get());
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testLongIterator() {
      Cache<Long, String> cache = getCache(0);

      testLongOperation(() -> {
         PrimitiveIterator.OfLong iterator = createStream(cache.entrySet()).mapToLong(toLong).iterator();
         AtomicLong count = new AtomicLong();
         iterator.forEachRemaining((long e) -> {
            assertTrue(cache.containsKey(e));
            count.addAndGet(e);
         });
         return count.get();
      }, cache);
   }

   public void testLongSortedIterator() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfLong iterator = createStream(entrySet).flatMapToLong(
            e -> LongStream.of(e.getKey(), e.getValue().length())).iterator();

      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         long next = iterator.nextLong();
         pos++;
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

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      assertFalse(createStream(entrySet).mapToLong(toLong).noneMatch(i -> i % 2 == 0));
      assertTrue(createStream(entrySet).mapToLong(toLong).noneMatch(i -> i > 10 && i < 0));
      assertFalse(createStream(entrySet).mapToLong(toLong).noneMatch(i -> i < 12));
   }

   public void testLongReduce1() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToLong(toLong).reduce(1, (i1, i2) -> i1 * i2));

      assertEquals(362880, createStream(entrySet).mapToLong(toLong).filter(i -> i != 0).reduce(1, (i1, i2) -> i1 * i2));
   }

   public void testLongReduce2() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      // One value is 0 so multiplying them together should be 0
      assertEquals(0, createStream(entrySet).mapToLong(toLong).reduce((i1, i2) -> i1 * i2).getAsLong());

      assertEquals(362880, createStream(entrySet).mapToLong(toLong).filter(i -> i != 0).reduce((i1, i2) -> i1 * i2)
            .getAsLong());
   }

   public void testLongSummaryStatistics() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(9, createStream(entrySet).mapToLong(toLong).max().getAsLong());
   }

   public void testLongMin() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      assertEquals(0, createStream(entrySet).mapToLong(toLong).min().getAsLong());
   }

   public void testLongSortedSkip() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).forEach(i -> cache.put(i, i + "-value"));
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      for (int i = 0; i < range; i++) {
         LongSummaryStatistics stats = createStream(entrySet).mapToLong(toLong)
               .sorted().skip(i).summaryStatistics();
         assertEquals(range - i, stats.getCount());
         assertEquals(i, stats.getMin());
         assertEquals(range - 1, stats.getMax());
         assertEquals(IntStream.range(i, range).sum(), stats.getSum());
      }
   }

   public void testLongSortedLimit() {
      Cache<Long, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      LongStream.range(0, range).forEach(i -> cache.put(i, i + "-value"));
      CacheSet<Map.Entry<Long, String>> entrySet = cache.entrySet();

      for (int i = 1; i < range; i++) {
         LongSummaryStatistics stats = createStream(entrySet).mapToLong(toLong)
               .sorted().limit(i).summaryStatistics();
         assertEquals(i, stats.getCount());
         assertEquals(0, stats.getMin());
         assertEquals(i - 1, stats.getMax());
         assertEquals(IntStream.range(0, i).sum(), stats.getSum());
      }
   }

   // DoubleStream tests

   static final SerializableToDoubleFunction<Map.Entry<Double, String>> toDouble = Map.Entry::getKey;

   public void testDoubleAllMatch() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertFalse(createStream(entrySet).mapToDouble(toDouble).allMatch(i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).allMatch(i -> i > 5 && i < 0));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).allMatch(i -> i < 5));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).allMatch(i -> Math.floor(i) == i));
   }

   public void testDoubleAnyMatch() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToDouble(toDouble).anyMatch(i -> i % 2 == 0));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).anyMatch(i -> i > 5 && i < 0));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).anyMatch(i -> i < 5));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).anyMatch(i -> Math.floor(i) == i));
   }

   public void testDoubleAverage() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(2.25, createStream(entrySet).mapToDouble(toDouble).average().getAsDouble());
   }

   public void testDoubleCollect() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      HashSet<Double> set = createStream(entrySet).mapToDouble(toDouble).collect(HashSet::new,
            Set::add, Set::addAll);
      assertEquals(10, set.size());
   }

   public void testDoubleCount() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(10, createStream(entrySet).mapToDouble(toDouble).count());
   }

   public void testDoubleFindAny() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertTrue(createStream(entrySet).mapToDouble(toDouble).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToDouble(toDouble).filter(e -> e % 2 == 0).findAny().isPresent());
      assertTrue(createStream(entrySet).mapToDouble(toDouble).filter(e -> e < 5 && e >= 0).findAny().isPresent());
      assertFalse(createStream(entrySet).mapToDouble(toDouble).filter(e -> e > 5).findAny().isPresent());
   }

   public void testDoubleFindFirst() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).sorted().findFirst().getAsDouble());
   }

   public void testDoubleForEach() {
      Cache<Double, String> cache = getCache(0);

      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());
      try {
         testDoubleOperation(() -> {
            createStream(cache.entrySet()).mapToDouble(toDouble).forEach(e -> {
               DoubleSummaryStatistics stats = getForEachObject(offset);
               synchronized (stats) {
                  stats.accept(e);
               }
            });
            return getForEachObject(offset);
         }, cache);
      } finally {
         clearForEachObject(offset);
      }
   }

   public void testDoubleFlatMapForEach() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToDouble(toDouble).flatMap(e -> DoubleStream.of(e, 2.25))
               .forEach(e -> {
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

   public static class ForEachDoubleInjected<E> extends AbstractForEach implements DoubleConsumer {

      @ProtoFactory
      ForEachDoubleInjected(int cacheOffset, int atomicOffset) {
         super(cacheOffset, atomicOffset);
      }

      @Override
      public void accept(double value) {
         Cache<?, ?> cache = getForEachObject(cacheOffset);
         if (cache != null && this.cache != null && cache.getName().equals(this.cache.getName())) {
            DoubleSummaryStatistics stats = getForEachObject(atomicOffset);
            synchronized (stats) {
               stats.accept(value);
            }
         } else {
            fail("Did not receive correct cache!");
         }
      }
   }

   private void testDoubleOperation(Supplier<DoubleSummaryStatistics> statisticsSupplier, Cache<Double, String> cache) {
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));


      DoubleSummaryStatistics stats = statisticsSupplier.get();
      assertEquals(2.25, stats.getAverage());
      assertEquals(0.0, stats.getMin());
      assertEquals(4.5, stats.getMax());
      assertEquals(10, stats.getCount());
      assertEquals(22.5, stats.getSum());
   }

   public void testDoubleForEachCacheInjected() {
      Cache<Double, String> cache = getCache(0);
      int cacheOffset = populateNextForEachStructure(cache);
      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());

      try {
         testDoubleOperation(() -> {
            createStream(cache.entrySet()).mapToDouble(toDouble).forEach(new ForEachDoubleInjected<>(cacheOffset,
                  offset));
            return getForEachObject(offset);
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(offset);
      }
   }

   public void testDoubleForEachBiConsumer() {
      Cache<Double, String> cache = getCache(0);
      int cacheOffset = populateNextForEachStructure(cache);
      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());

      try {
         testDoubleOperation(() -> {
            createStream(cache.entrySet()).mapToDouble(toDouble).forEach((c, d) -> {
               Cache<?, ?> localCache = getForEachObject(cacheOffset);
               if (c != null && localCache != null && c.getName().equals(localCache.getName())) {
                  DoubleSummaryStatistics stats = getForEachObject(offset);
                  synchronized (stats) {
                     stats.accept(d);
                  }
               }
            });
            DoubleSummaryStatistics stats = getForEachObject(offset);
            return stats;
         }, cache);
      } finally {
         clearForEachObject(cacheOffset);
         clearForEachObject(offset);
      }
   }

   public void testDoubleFlatMapObjConsumerForEach() {
      Cache<Double, String> cache = getCache(0);
      String cacheName = cache.getName();
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      int offset = populateNextForEachStructure(new DoubleSummaryStatistics());
      try {
         createStream(entrySet).distributedBatchSize(5).mapToDouble(toDouble).flatMap(e -> DoubleStream.of(e, 2.25))
               .forEach((c, e) -> {
                  assertEquals(cacheName, c.getName());
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

   public void testDoubleIterator() {
      Cache<Double, String> cache = getCache(0);
      testDoubleOperation(() -> {
         PrimitiveIterator.OfDouble iterator = createStream(cache.entrySet()).mapToDouble(toDouble).iterator();
         DoubleSummaryStatistics doubleSummaryStatistics = new DoubleSummaryStatistics();
         iterator.forEachRemaining((double e) -> {
            assertTrue(cache.containsKey(e));
            doubleSummaryStatistics.accept(e);
         });
         return doubleSummaryStatistics;
      }, cache);
   }

   public void testDoubleSortedIterator() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      PrimitiveIterator.OfDouble iterator = createStream(entrySet).flatMapToDouble(
            e -> DoubleStream.of(e.getKey(), .5)).iterator();

      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         double next = iterator.nextDouble();
         pos++;
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

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // This isn't the best usage of this, but should be a usable example
      assertFalse(createStream(entrySet).mapToDouble(toDouble).noneMatch(i -> i % 2 == 0));
      assertTrue(createStream(entrySet).mapToDouble(toDouble).noneMatch(i -> i > 5 && i < 0));
      assertFalse(createStream(entrySet).mapToDouble(toDouble).noneMatch(i -> i < 5));
   }

   public void testDoubleReduce1() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // One value is 0.0 so multiplying them together should be 0.0
      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).reduce(1.0, (i1, i2) -> i1 * i2));

      assertEquals(708.75, createStream(entrySet).mapToDouble(toDouble).filter(i -> i != 0).reduce(1.0,
            (i1, i2) -> i1 * i2));
   }

   public void testDoubleReduce2() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      // One value is 0.0 so multiplying them together should be 0.0
      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).reduce((i1, i2) -> i1 * i2).getAsDouble());

      assertEquals(708.75, createStream(entrySet).mapToDouble(toDouble).filter(i -> i != 0)
            .reduce((i1, i2) -> i1 * i2).getAsDouble());
   }

   public void testDoubleSummaryStatistics() {
      Cache<Double, String> cache = getCache(0);
      testDoubleOperation(() -> createStream(cache.entrySet()).mapToDouble(toDouble).summaryStatistics(), cache);
   }

   public void testDoubleToArray() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

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

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(4.5, createStream(entrySet).mapToDouble(toDouble).max().getAsDouble());
   }

   public void testDoubleMin() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      DoubleStream.iterate(0.0, d -> d + .5).limit(10).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      assertEquals(0.0, createStream(entrySet).mapToDouble(toDouble).min().getAsDouble());
   }

   public void testDoubleSortedSkip() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).mapToDouble(value -> value).forEach(i -> cache.put(i, i + "-value"));
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      for (int i = 0; i < range; i++) {
         DoubleSummaryStatistics stats = createStream(entrySet).mapToDouble(toDouble)
               .sorted().skip(i).summaryStatistics();
         assertEquals(range - i, stats.getCount());
         assertEquals((double) i, stats.getMin());
         assertEquals((double) range - 1, stats.getMax());
         assertEquals((double) IntStream.range(i, range).sum(), stats.getSum());
      }
   }

   public void testDoubleSortedLimit() {
      Cache<Double, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).mapToDouble(value -> value).forEach(i -> cache.put(i, i + "-value"));
      CacheSet<Map.Entry<Double, String>> entrySet = cache.entrySet();

      for (int i = 1; i < range; i++) {
         DoubleSummaryStatistics stats = createStream(entrySet).mapToDouble(toDouble)
               .sorted().limit(i).summaryStatistics();
         assertEquals(i, stats.getCount());
         assertEquals(0d, stats.getMin());
         assertEquals((double) i - 1, stats.getMax());
         assertEquals((double) IntStream.range(0, i).sum(), stats.getSum());
      }
   }

   // KeySet Tests
   public void testObjKeySetMax() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Integer> keySet = cache.keySet();

      assertEquals(9, createStream(keySet).max(Integer::compare).get().intValue());
   }

   public void testKeySetIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Integer> keySet = cache.keySet();

      Iterator<Integer> iterator = createStream(keySet).iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> {
         assertTrue(cache.containsKey(e));
         count.addAndGet(e);
      });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testKeySetMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Integer> keySet = cache.keySet();

      Iterator<String> iterator = createStream(keySet).map(i -> i + "-value").iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> {
         Integer key = Integer.valueOf(e.substring(0, 1));
         assertEquals(cache.get(key), e);
         count.addAndGet(key);
      });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testKeySetFlatMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Integer> keySet = cache.keySet();

      PrimitiveIterator.OfInt iterator = createStream(keySet).flatMapToInt(
            i -> IntStream.of(i, 3)).iterator();
      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         int next = iterator.nextInt();
         if (next == 3) {
            halfCount++;
         }
         pos++;
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

      CacheCollection<String> keySet = cache.values();

      assertEquals("9-value",
            createStream(keySet).max((e1, e2) -> Integer.compare(
                  Integer.valueOf(e1.substring(0, 1)),
                  Integer.valueOf(e2.substring(0, 1)))).get());
   }

   // Tests to make sure when max returns an empty result that it works
   public void testObjMaxEmpty() {
      Cache<Integer, String> cache = getCache(0);
      assertEquals(0, cache.size());
      CacheCollection<String> keySet = cache.values();

      assertFalse(
            createStream(keySet).max((e1, e2) -> Integer.compare(
                  Integer.valueOf(e1.substring(0, 1)),
                  Integer.valueOf(e2.substring(0, 1)))).isPresent());
   }

   public void testObjValuesIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheCollection<String> values = cache.values();

      Iterator<String> iterator = createStream(values).iterator();
      AtomicInteger count = new AtomicInteger();
      iterator.forEachRemaining(e -> {
         Integer key = Integer.valueOf(e.substring(0, 1));
         assertEquals(cache.get(key), e);
         count.addAndGet(key);
      });
      assertEquals((range - 1) * (range / 2), count.get());
   }

   public void testValuesMapIterator() {
      Cache<Integer, String> cache = getCache(0);
      int range = 10;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheCollection<String> values = cache.values();

      PrimitiveIterator.OfInt iterator = createStream(values).mapToInt(
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

      CacheCollection<String> values = cache.values();

      PrimitiveIterator.OfInt iterator = createStream(values).flatMapToInt(
            e -> IntStream.of(Integer.valueOf(e.substring(0, 1)), e.length())).iterator();

      int pos = 0;
      int halfCount = 0;
      while (iterator.hasNext()) {
         int next = iterator.nextInt();
         if (next == 7) {
            halfCount++;
         }
         pos++;
         assertTrue(cache.containsKey(next));
      }
      assertEquals(range + 1, halfCount);
      assertEquals(range * 2, pos);
   }

   public void testKeySegmentFilter() {
      Cache<Integer, String> cache = getCache(0);
      int range = 12;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      // Take the first half of the segments
      int segments = cache.getCacheConfiguration().clustering().hash().numSegments() / 2;
      AtomicInteger realCount = new AtomicInteger();

      KeyPartitioner keyPartitioner = ComponentRegistry.componentOf(cache, KeyPartitioner.class);
      cache.forEach((k, v) -> {
         if (segments >= keyPartitioner.getSegment(k)) {
            realCount.incrementAndGet();
         }
      });
      IntSet intSet = IntSets.from(IntStream.range(0, segments).boxed().collect(Collectors.toSet()));
      assertEquals(realCount.get(), createStream(entrySet).filterKeySegments(intSet).count());
   }

   public void testKeyFilter() {
      Cache<Integer, String> cache = getCache(0);
      int range = 12;
      // First populate the cache with a bunch of values
      IntStream.range(0, range).boxed().forEach(i -> cache.put(i, i + "-value"));

      CacheSet<Map.Entry<Integer, String>> entrySet = cache.entrySet();

      Set<Integer> keys = IntStream.of(2, 5, 8, 3, 1, range + 2).boxed().collect(Collectors.toSet());
      assertEquals(keys.size() - 1, createStream(entrySet).filterKeys(keys).count());
   }

   @ProtoAdapter(ConcurrentHashMap.class)
   public static class ConcurrentHashMapAdapter<K, V> {
      @ProtoFactory
      static <K, V> ConcurrentHashMap<K, V> create(List<KeyValuePair<K, V>> pairs) {
         ConcurrentHashMap<K, V> map = new ConcurrentHashMap<>(pairs.size());
         for (var kvp : pairs)
            map.put(kvp.getKey(), kvp.getValue());
         return map;
      }

      @ProtoField(1)
      List<KeyValuePair<K, V>> getPairs(ConcurrentHashMap<K, V> map) {
         return map.entrySet()
               .stream()
               .map(e -> new KeyValuePair<>(e.getKey(), e.getValue()))
               .collect(Collectors.toList());
      }
   }

   @ProtoSchema(
         dependsOn = GlobalContextInitializer.class,
         includeClasses = {
               ConcurrentHashMapAdapter.class,
               ForEachDoubleInjected.class,
               ForEachInjected.class,
               ForEachIntInjected.class,
               ForEachLongInjected.class,
               KeyValuePairArrayAdapter.class
         },
         schemaFileName = "test.core.BaseStreamTest.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.BaseStreamTest",
         service = false,
         syntax = ProtoSyntax.PROTO3
   )
   public interface BaseStreamTestSCI extends SerializationContextInitializer {
      SerializationContextInitializer INSTANCE = new BaseStreamTestSCIImpl();
   }
}
