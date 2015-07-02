package org.infinispan.functional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.EntryView.WriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.MetaParam.EntryVersionParam;
import org.infinispan.commons.api.functional.MetaParam.Lifespan;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.infinispan.commons.api.functional.EntryVersion.CompareResult.EQUAL;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.infinispan.util.functional.MarshallableFunctionalInterfaces.*;
import static org.testng.AssertJUnit.*;

/**
 * Test suite for verifying basic functional map functionality,
 * and for testing out functionality that is not available via standard
 * {@link java.util.concurrent.ConcurrentMap} nor JSR-107 JCache
 * APIs, such as atomic conditional metadata-based replace operations, which
 * are required by Hot Rod.
 */
@Test(groups = "functional", testName = "functional.FunctionalMapTest")
public class FunctionalMapTest extends MultipleCacheManagersTest {

   private static final String DIST = "dist";
   private static final String REPL = "repl";
   private static final Random R = new Random();

   FunctionalMapImpl<Integer, String> local1;
   FunctionalMapImpl<Integer, String> local2;

   FunctionalMapImpl<Object, String> dist1;
   FunctionalMapImpl<Object, String> dist2;

   FunctionalMapImpl<Object, String> repl1;
   FunctionalMapImpl<Object, String> repl2;

//   private ReadOnlyMap<Integer, String> readOnlyMap;
//   private WriteOnlyMap<Integer, String> writeOnlyMap;
//   private ReadWriteMap<Integer, String> readWriteMap;

   <K> ReadOnlyMap<K, String> ro(FunctionalMapImpl<K, String> fmap) {
      return ReadOnlyMapImpl.create(fmap);
   }

   <K> WriteOnlyMap<K, String> wo(FunctionalMapImpl<K, String> fmap) {
      return WriteOnlyMapImpl.create(fmap);
   }

   <K> ReadWriteMap<K, String> rw(FunctionalMapImpl<K, String> fmap) {
      return ReadWriteMapImpl.create(fmap);
   }

   Supplier<Integer> supplyIntKey() {
      return () -> R.nextInt(Integer.MAX_VALUE);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // Create local caches as default in a cluster of 2
      createClusteredCaches(2, new ConfigurationBuilder());
      // Create distributed caches
      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(DIST, distBuilder.build()));
      // Create replicated caches
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(REPL, replBuilder.build()));
      // Wait for cluster to form
      waitForClusterToForm(DIST, REPL);
   }

   @BeforeClass
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      local1 = FunctionalMapImpl.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      local2 = FunctionalMapImpl.create(cacheManagers.get(0).<Integer, String>getCache().getAdvancedCache());
      dist1 = FunctionalMapImpl.create(cacheManagers.get(0).<Object, String>getCache(DIST).getAdvancedCache());
      dist2 = FunctionalMapImpl.create(cacheManagers.get(1).<Object, String>getCache(DIST).getAdvancedCache());
      repl1 = FunctionalMapImpl.create(cacheManagers.get(0).<Object, String>getCache(REPL).getAdvancedCache());
      repl2 = FunctionalMapImpl.create(cacheManagers.get(1).<Object, String>getCache(REPL).getAdvancedCache());
   }

//   @Override
//   protected EmbeddedCacheManager createCacheManager() throws Exception {
//      return TestCacheManagerFactory.createCacheManager();
//   }

//   @Override
//   protected void setup() throws Exception {
//      super.setup();
//      AdvancedCache<Integer, String> advCache = cacheManager.<Integer, String>getCache().getAdvancedCache();
//      FunctionalMapImpl<Integer, String> functionalMap = FunctionalMapImpl.create(advCache);
//      readOnlyMap = ReadOnlyMapImpl.create(functionalMap);
//      writeOnlyMap = WriteOnlyMapImpl.create(functionalMap);
//      readWriteMap = ReadWriteMapImpl.create(functionalMap);
//   }

   public void testLocalReadOnlyGetsEmpty() {
      doReadOnlyGetsEmpty(supplyIntKey(), ro(local1));
   }

   public void testReplReadOnlyGetsEmpty() {
      doReadOnlyGetsEmpty(supplyKeyForCache(0, REPL), ro(repl1));
      doReadOnlyGetsEmpty(supplyKeyForCache(1, REPL), ro(repl1));
   }

   public void testDistReadOnlyGetsEmpty() {
      doReadOnlyGetsEmpty(supplyKeyForCache(0, DIST), ro(dist1));
      doReadOnlyGetsEmpty(supplyKeyForCache(1, DIST), ro(dist1));
   }

   /**
    * Read-only allows to retrieve an empty cache entry.
    */
   private <K> void doReadOnlyGetsEmpty(Supplier<K> keySupplier, ReadOnlyMap<K, String> map) {
      K key = keySupplier.get();
      await(map.eval(key, ReadEntryView::find).thenAccept(v -> assertEquals(Optional.empty(), v)));
   }

   public void testLocalWriteConstantAndReadGetsValue() {
      doWriteConstantAndReadGetsValue(supplyIntKey(), ro(local1), wo(local2));
   }

   public void testReplWriteConstantAndReadGetsValue() {
      ReadOnlyMap<Object, String> ro1 = ro(repl1);
      WriteOnlyMap<Object, String> wo2 = wo(repl2);
      doWriteConstantAndReadGetsValue(supplyKeyForCache(0, REPL), ro1, wo2);
      doWriteConstantAndReadGetsValue(supplyKeyForCache(1, REPL), ro1, wo2);
   }

   public void testDistWriteConstantAndReadGetsValue() {
      ReadOnlyMap<Object, String> ro1 = ro(dist1);
      WriteOnlyMap<Object, String> wo2 = wo(dist2);
      doWriteConstantAndReadGetsValue(supplyKeyForCache(0, DIST), ro1, wo2);
      doWriteConstantAndReadGetsValue(supplyKeyForCache(1, DIST), ro1, wo2);
   }

   /**
    * Write-only allows for constant, non-capturing, values to be written,
    * and read-only allows for those values to be retrieved.
    */
   private <K> void doWriteConstantAndReadGetsValue(Supplier<K> keySupplier,
         ReadOnlyMap<K, String> map1, WriteOnlyMap<K, String> map2) {
      K key = keySupplier.get();
      await(
         map2.eval(key, SetStringConstant.INSTANCE).thenCompose(r ->
               map1.eval(key, ReadEntryView::get).thenAccept(v -> {
                     assertNull(r);
                     assertEquals("one", v);
                  }
               )
         )
      );
   }

   @SerializeWith(value = SetStringConstant.Externalizer0.class)
   private static final class SetStringConstant implements Consumer<WriteEntryView<String>> {
      @Override
      public void accept(WriteEntryView<String> wo) {
         wo.set("one");
      }

      private static final SetStringConstant INSTANCE = new SetStringConstant();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   public void testLocalWriteValueAndReadValueAndMetadata() {
      doWriteValueAndReadValueAndMetadata(supplyIntKey(), ro(local1), wo(local2));
   }

   public void testReplWriteValueAndReadValueAndMetadata() {
      ReadOnlyMap<Object, String> ro1 = ro(repl1);
      WriteOnlyMap<Object, String> wo2 = wo(repl2);
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(0, REPL), ro1, wo2);
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(1, REPL), ro1, wo2);
   }

   public void testDistWriteValueAndReadValueAndMetadata() {
      ReadOnlyMap<Object, String> ro1 = ro(dist1);
      WriteOnlyMap<Object, String> wo2 = wo(dist2);
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(0, DIST), ro1, wo2);
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(1, DIST), ro1, wo2);
   }

   /**
    * Write-only allows for non-capturing values to be written along with metadata,
    * and read-only allows for both values and metadata to be retrieved.
    */
   private <K> void doWriteValueAndReadValueAndMetadata(Supplier<K> keySupplier,
         ReadOnlyMap<K, String> map1, WriteOnlyMap<K, String> map2) {
      K key = keySupplier.get();
      await(
         map2.eval(key, "one", SetValueAndConstantLifespan.getInstance()).thenCompose(r ->
               map1.eval(key, ro -> ro).thenAccept(ro -> {
                     assertNull(r);
                     assertEquals(Optional.of("one"), ro.find());
                     assertEquals("one", ro.get());
                     assertEquals(Optional.of(new Lifespan(100000)), ro.findMetaParam(Lifespan.ID));
                     assertEquals(new Lifespan(100000), ro.getMetaParam(Lifespan.ID));
                  }
               )
         )
      );
   }

   @SerializeWith(value = SetValueAndConstantLifespan.Externalizer0.class)
   private static final class SetValueAndConstantLifespan<V>
         implements BiConsumer<V, WriteEntryView<V>> {
      @Override
      public void accept(V v, WriteEntryView<V> wo) {
         wo.set(v, new Lifespan(100000));
      }

      @SuppressWarnings("unchecked")
      private static <V> SetValueAndConstantLifespan<V> getInstance() {
         return INSTANCE;
      }

      private static final SetValueAndConstantLifespan INSTANCE =
         new SetValueAndConstantLifespan<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   public void testLocalReadWriteGetsEmpty() {
      doReadWriteGetsEmpty(supplyIntKey(), rw(local1));
   }

   public void testReplReadWriteGetsEmpty() {
      doReadWriteGetsEmpty(supplyKeyForCache(0, REPL), rw(repl1));
      doReadWriteGetsEmpty(supplyKeyForCache(1, REPL), rw(repl1));
   }

   public void testDistReadWriteGetsEmpty() {
      doReadWriteGetsEmpty(supplyKeyForCache(0, DIST), rw(dist1));
      doReadWriteGetsEmpty(supplyKeyForCache(1, DIST), rw(dist1));
   }

   /**
    * Read-write allows to retrieve an empty cache entry.
    */
   private <K> void doReadWriteGetsEmpty(Supplier<K> keySupplier, ReadWriteMap<K, String> map) {
      K key = keySupplier.get();
      await(map.eval(key, findReadWrite()).thenAccept(v -> assertEquals(Optional.empty(), v)));
   }

   public void testLocalReadWriteValuesReturnPrevious() {
      doReadWriteConstantReturnPrev(supplyIntKey(), rw(local1), rw(local2));
   }

   public void testReplReadWriteValuesReturnPrevious() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(0, REPL), rw(repl1), rw(repl2));
      doReadWriteConstantReturnPrev(supplyKeyForCache(1, REPL), rw(repl1), rw(repl2));
   }

   public void testDistReadWriteValuesReturnPrevious() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(0, DIST), rw(dist1), rw(dist2));
      doReadWriteConstantReturnPrev(supplyKeyForCache(1, DIST), rw(dist1), rw(dist2));
   }

   /**
    * Read-write allows for constant, non-capturing, values to be written,
    * returns previous value, and also allows values to be retrieved.
    */
   private <K> void doReadWriteConstantReturnPrev(Supplier<K> keySupplier,
         ReadWriteMap<K, String> map1, ReadWriteMap<K, String> map2) {
      K key = keySupplier.get();
      await(
         map2.eval(key, SetStringConstantReturnPrevious.getInstance()).thenCompose(r ->
               map1.eval(key, getReadWrite()).thenAccept(v -> {
                     assertFalse(r.isPresent());
                     assertEquals("one", v);
                  }
               )
         )
      );
   }

   @SerializeWith(value = SetStringConstantReturnPrevious.Externalizer0.class)
   private static final class SetStringConstantReturnPrevious<K>
         implements Function<ReadWriteEntryView<K, String>, Optional<String>> {
      @Override
      public Optional<String> apply(ReadWriteEntryView<K, String> rw) {
         Optional<String> prev = rw.find();
         rw.set("one");
         return prev;
      }

      @SuppressWarnings("unchecked")
      private static <K> SetStringConstantReturnPrevious<K> getInstance() {
         return INSTANCE;
      }

      private static final SetStringConstantReturnPrevious INSTANCE =
         new SetStringConstantReturnPrevious<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   public void testLocalReadWriteForConditionalParamBasedReplace() {
      doReadWriteForConditionalParamBasedReplace(supplyIntKey(), rw(local1), rw(local2));
   }

   public void testReplReadWriteForConditionalParamBasedReplace() {
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(0, REPL), rw(repl1), rw(repl2));
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(1, REPL), rw(repl1), rw(repl2));
   }

   public void testDistReadWriteForConditionalParamBasedReplace() {
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(0, DIST), rw(dist1), rw(dist2));
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(1, DIST), rw(dist1), rw(dist2));
   }

   /**
    * Read-write allows for replace operations to happen based on version
    * comparison, and update version information if replace is versions are
    * equals.
    *
    * This is the kind of advance operation that Hot Rod prototocol requires
    * but the current Infinispan API is unable to offer without offering
    * atomicity at the level of the function that compares the version
    * information.
    */
   private <K> void doReadWriteForConditionalParamBasedReplace(Supplier<K> keySupplier,
         ReadWriteMap<K, String> map1, ReadWriteMap<K, String> map2) {
      replaceWithVersion(keySupplier, map1, map2, 100, rw -> {
            assertEquals("uno", rw.get());
            assertEquals(new EntryVersionParam<>(new NumericEntryVersion(200)),
               rw.getMetaParam(EntryVersionParam.ID()));
         }
      );
      replaceWithVersion(keySupplier, map1, map2, 900, rw -> {
         assertEquals(Optional.of("one"), rw.find());
         assertEquals(Optional.of(new EntryVersionParam<>(new NumericEntryVersion(100))),
            rw.findMetaParam(EntryVersionParam.ID()));
      });
   }

   private <K> void replaceWithVersion(Supplier<K> keySupplier,
         ReadWriteMap<K, String> map1, ReadWriteMap<K, String> map2,
         long version, Consumer<ReadWriteEntryView<K, String>> asserts) {
      K key = keySupplier.get();
      await(
         map1.eval(key, SetStringAndVersionConstant.getInstance()).thenCompose(r ->
            map2.eval(key, new VersionBasedConditionalReplace<>(version)).thenAccept(rw -> {
                  assertNull(r);
                  asserts.accept(rw);
               }
            )
         )
      );
   }

   @SerializeWith(value = SetStringAndVersionConstant.Externalizer0.class)
   private static final class SetStringAndVersionConstant<K>
         implements Function<ReadWriteEntryView<K, String>, Void> {
      @Override
      public Void apply(ReadWriteEntryView<K, String> rw) {
         rw.set("one", new EntryVersionParam<>(new NumericEntryVersion(100)));
         return null;
      }

      @SuppressWarnings("unchecked")
      private static <K> SetStringAndVersionConstant<K> getInstance() {
         return INSTANCE;
      }

      private static final SetStringAndVersionConstant INSTANCE =
         new SetStringAndVersionConstant<>();
      public static final class Externalizer0 implements Externalizer<Object> {
         public void writeObject(ObjectOutput oo, Object o) {}
         public Object readObject(ObjectInput input) { return INSTANCE; }
      }
   }

   @SerializeWith(value = VersionBasedConditionalReplace.Externalizer0.class)
   private static final class VersionBasedConditionalReplace<K>
      implements Function<ReadWriteEntryView<K, String>, ReadWriteEntryView<K, String>> {
      private final long version;

      private VersionBasedConditionalReplace(long version) {
         this.version = version;
      }

      @Override
      public ReadWriteEntryView<K, String> apply(ReadWriteEntryView<K, String> rw) {
         EntryVersionParam<Long> versionParam = rw.getMetaParam(EntryVersionParam.ID());
         if (versionParam.get().compareTo(new NumericEntryVersion(version)) == EQUAL)
            rw.set("uno", new EntryVersionParam<>(new NumericEntryVersion(200)));
         return rw;
      }

      public static final class Externalizer0 implements Externalizer<VersionBasedConditionalReplace<?>> {
         @Override
         public void writeObject(ObjectOutput output, VersionBasedConditionalReplace<?> object) throws IOException {
            output.writeLong(object.version);
         }

         @Override
         public VersionBasedConditionalReplace<?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            long version = input.readLong();
            return new VersionBasedConditionalReplace<>(version);
         }
      }
   }

   public void testAutoClose() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() throws Exception {
            AdvancedCache<?, ?> readOnlyCache = cm.getCache("read-only").getAdvancedCache();
            try (ReadOnlyMap<?, ?> ro = ReadOnlyMapImpl.create(FunctionalMapImpl.create(readOnlyCache))) {
               assertNotNull(ro); // No-op, just verify that it implements AutoCloseable
            }
            assertTrue(readOnlyCache.getStatus().isTerminated());

            AdvancedCache<?, ?> writeOnlyCache = cm.getCache("write-only").getAdvancedCache();
            try (WriteOnlyMap<?, ?> wo = WriteOnlyMapImpl.create(FunctionalMapImpl.create(writeOnlyCache))) {
               assertNotNull(wo); // No-op, just verify that it implements AutoCloseable
            }
            assertTrue(writeOnlyCache.getStatus().isTerminated());

            AdvancedCache<?, ?> readWriteCache = cm.getCache("read-write").getAdvancedCache();
            try (ReadWriteMap<?, ?> rw = ReadWriteMapImpl.create(FunctionalMapImpl.create(readWriteCache))) {
               assertNotNull(rw); // No-op, just verify that it implements AutoCloseable
            }
            assertTrue(readWriteCache.getStatus().isTerminated());
         }
      });
   }

   public void testLocalReadOnlyEvalManyEmpty() {
      doReadOnlyEvalManyEmpty(supplyIntKey(), ro(local1));
   }

   public void testReplReadOnlyEvalManyEmpty() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(0, REPL), ro(repl1));
      doReadOnlyEvalManyEmpty(supplyKeyForCache(1, REPL), ro(repl1));
   }

   public void testDistReadOnlyEvalManyEmpty() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(0, DIST), ro(dist1));
      doReadOnlyEvalManyEmpty(supplyKeyForCache(1, DIST), ro(dist1));
   }

   private <K> void doReadOnlyEvalManyEmpty(Supplier<K> keySupplier, ReadOnlyMap<K, String> map) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Traversable<ReadEntryView<K, String>> t = map
         .evalMany(new HashSet<>(Arrays.asList(key1, key2, key3)), ro -> ro);
      t.forEach(ro -> assertFalse(ro.find().isPresent()));
   }

   public void testLocalUpdateSubsetAndReturnPrevs() {
      doUpdateSubsetAndReturnPrevs(supplyIntKey(), ro(local1), wo(local2), rw(local2));
   }

   public void testReplUpdateSubsetAndReturnPrevs() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(0, REPL), ro(repl1), wo(repl2), rw(repl2));
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(1, REPL), ro(repl1), wo(repl2), rw(repl2));
   }

   public void testDistUpdateSubsetAndReturnPrevs() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(0, DIST), ro(dist1), wo(dist2), rw(dist2));
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(1, DIST), ro(dist1), wo(dist2), rw(dist2));
   }

   private <K> void doUpdateSubsetAndReturnPrevs(Supplier<K> keySupplier,
         ReadOnlyMap<K, String> map1, WriteOnlyMap<K, String> map2, ReadWriteMap<K, String> map3) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      consume(map2.evalMany(data, setValueConsumer()));
      Traversable<String> currentValues = map1.evalMany(data.keySet(), ro -> ro.get());
      List<String> collectedValues = currentValues.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(collectedValues, new ArrayList<>(data.values()));

      Map<K, String> newData = new HashMap<>();
      newData.put(key1, "bat");
      newData.put(key2, "bi");
      newData.put(key3, "hiru");
      Traversable<String> prevTraversable = map3.evalMany(newData, setValueReturnPrevOrNull());
      List<String> collectedPrev = prevTraversable.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(new ArrayList<>(data.values()), collectedPrev);

      Traversable<String> updatedValues = map1.evalMany(data.keySet(), ro -> ro.get());
      List<String> collectedUpdates = updatedValues.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(new ArrayList<>(newData.values()), collectedUpdates);
   }

   public void testLocalReadWriteToRemoveAllAndReturnPrevs() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyIntKey(), wo(local1), rw(local2));
   }

   public void testReplReadWriteToRemoveAllAndReturnPrevs() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(0, REPL), wo(repl1), rw(repl2));
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(1, REPL), wo(repl1), rw(repl2));
   }

   public void testDistReadWriteToRemoveAllAndReturnPrevs() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(0, DIST), wo(dist1), rw(dist2));
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(1, DIST), wo(dist1), rw(dist2));
   }

   private <K> void doReadWriteToRemoveAllAndReturnPrevs(Supplier<K> keySupplier,
         WriteOnlyMap<K, String> map1, ReadWriteMap<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      consume(map1.evalMany(data, setValueConsumer()));
      Traversable<String> prevTraversable = map2.evalAll(removeReturnPrevOrNull());
      Set<String> prevValues = prevTraversable.collect(HashSet::new, HashSet::add, HashSet::addAll);
      assertEquals(new HashSet<>(data.values()), prevValues);
   }

   private static void consume(CloseableIterator<Void> it) {
      while (it.hasNext())
         it.next();
   }

   private static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

}
