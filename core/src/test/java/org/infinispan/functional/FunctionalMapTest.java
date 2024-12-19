package org.infinispan.functional;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;
import static org.infinispan.functional.FunctionalTestUtils.assertReadOnlyViewEmpty;
import static org.infinispan.functional.FunctionalTestUtils.assertReadOnlyViewEquals;
import static org.infinispan.functional.FunctionalTestUtils.assertReadWriteViewEmpty;
import static org.infinispan.functional.FunctionalTestUtils.assertReadWriteViewEquals;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.functional.FunctionalTestUtils.ro;
import static org.infinispan.functional.FunctionalTestUtils.rw;
import static org.infinispan.functional.FunctionalTestUtils.supplyIntKey;
import static org.infinispan.functional.FunctionalTestUtils.wo;
import static org.infinispan.marshall.core.MarshallableFunctions.identity;
import static org.infinispan.marshall.core.MarshallableFunctions.removeReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadOnlyFindOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadWriteFind;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadWriteGet;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadWriteView;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueConsumer;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueReturnView;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.MetaParam.MetaEntryVersion;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.function.SerializableFunction;
import org.testng.annotations.Test;

/**
 * Test suite for verifying basic functional map functionality,
 * and for testing out functionality that is not available via standard
 * {@link java.util.concurrent.ConcurrentMap}
 * APIs, such as atomic conditional metadata-based replace operations, which
 * are required by Hot Rod.
 */
@Test(groups = "functional", testName = "functional.FunctionalMapTest")
public class FunctionalMapTest extends AbstractFunctionalTest {

   public FunctionalMapTest() {
      this.serializationContextInitializer = new FunctionalMapSCIImpl();
   }

   public void testSimpleWriteConstantAndReadGetsValue() {
      checkSimpleCacheAvailable();
      doWriteConstantAndReadGetsValue(supplyIntKey(), ro(fmapS1), wo(fmapS2));
   }

   public void testLocalWriteConstantAndReadGetsValue() {
      doWriteConstantAndReadGetsValue(supplyIntKey(), ro(fmapL1), wo(fmapL2));
   }

   public void testReplWriteConstantAndReadGetsValueOnNonOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(0, REPL), ro(fmapR1), wo(fmapR2));
   }

   public void testReplWriteConstantAndReadGetsValueOnOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(1, REPL), ro(fmapR1), wo(fmapR2));
   }

   public void testDistWriteConstantAndReadGetsValueOnNonOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(0, DIST), ro(fmapD1), wo(fmapD2));
   }

   public void testDistWriteConstantAndReadGetsValueOnOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(1, DIST), ro(fmapD1), wo(fmapD2));
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
               map1.eval(key, returnReadOnlyFindOrNull()).thenAccept(v -> {
                     assertNull(r);
                     assertEquals("one", v);
                  }
               )
         )
      );
   }

   static final class SetStringConstant<K> implements Consumer<WriteEntryView<K, String>> {
      @Override
      public void accept(WriteEntryView<K, String> wo) {
         wo.set("one");
      }

      @SuppressWarnings("unchecked")
      @ProtoFactory
      static <K> SetStringConstant<K> getInstance() {
         return INSTANCE;
      }
      static final SetStringConstant INSTANCE = new SetStringConstant<>();
   }

   public void testSimpleWriteValueAndReadValueAndMetadata() {
      checkSimpleCacheAvailable();
      doWriteValueAndReadValueAndMetadata(supplyIntKey(), ro(fmapS1), wo(fmapS2));
   }

   public void testLocalWriteValueAndReadValueAndMetadata() {
      doWriteValueAndReadValueAndMetadata(supplyIntKey(), ro(fmapL1), wo(fmapL2));
   }

   public void testReplWriteValueAndReadValueAndMetadataOnNonOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(0, REPL), ro(fmapR1), wo(fmapR2));
   }

   public void testReplWriteValueAndReadValueAndMetadataOnOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(1, REPL), ro(fmapR1), wo(fmapR2));
   }

   public void testDistWriteValueAndReadValueAndMetadataOnNonOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(0, DIST), ro(fmapD1), wo(fmapD2));
   }

   public void testDistWriteValueAndReadValueAndMetadataOnOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(1, DIST), ro(fmapD1), wo(fmapD2));
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
               map1.eval(key, identity()).thenAccept(ro -> {
                     assertNull(r);
                     assertEquals(Optional.of("one"), ro.find());
                     assertEquals("one", ro.get());
                     assertEquals(Optional.of(new MetaLifespan(100000)), ro.findMetaParam(MetaLifespan.class));
                  }
               )
         )
      );
   }

   static final class SetValueAndConstantLifespan<K, V>
         implements BiConsumer<V, WriteEntryView<K, V>> {
      @Override
      public void accept(V v, WriteEntryView<K, V> wo) {
         wo.set(v, new MetaLifespan(100000));
      }

      @SuppressWarnings("unchecked")
      @ProtoFactory
      static <K, V> SetValueAndConstantLifespan<K, V> getInstance() {
         return INSTANCE;
      }

      private static final SetValueAndConstantLifespan INSTANCE =
         new SetValueAndConstantLifespan<>();
   }

   public void testSimpleReadWriteGetsEmpty() {
      checkSimpleCacheAvailable();
      doReadWriteGetsEmpty(supplyIntKey(), rw(fmapS1));
   }

   public void testLocalReadWriteGetsEmpty() {
      doReadWriteGetsEmpty(supplyIntKey(), rw(fmapL1));
   }

   public void testReplReadWriteGetsEmptyOnNonOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(0, REPL), rw(fmapR1));
   }

   public void testReplReadWriteGetsEmptyOnOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(1, REPL), rw(fmapR1));
   }

   public void testDistReadWriteGetsEmptyOnNonOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(0, DIST), rw(fmapD1));
   }

   public void testDistReadWriteGetsEmptyOnOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(1, DIST), rw(fmapD1));
   }

   /**
    * Read-write allows to retrieve an empty cache entry.
    */
   <K> void doReadWriteGetsEmpty(Supplier<K> keySupplier, ReadWriteMap<K, String> map) {
      K key = keySupplier.get();
      await(map.eval(key, returnReadWriteFind()).thenAccept(v -> assertEquals(Optional.empty(), v)));
   }

   public void testSimpleReadWriteValuesReturnPrevious() {
      checkSimpleCacheAvailable();
      doReadWriteConstantReturnPrev(supplyIntKey(), rw(fmapS1), rw(fmapS2));
   }

   public void testLocalReadWriteValuesReturnPrevious() {
      doReadWriteConstantReturnPrev(supplyIntKey(), rw(fmapL1), rw(fmapL2));
   }

   public void testReplReadWriteValuesReturnPreviousOnNonOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(0, REPL), rw(fmapR1), rw(fmapR2));
   }

   public void testReplReadWriteValuesReturnPreviousOnOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(1, REPL), rw(fmapR1), rw(fmapR2));
   }

   public void testDistReadWriteValuesReturnPreviousOnNonOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(0, DIST), rw(fmapD1), rw(fmapD2));
   }

   public void testDistReadWriteValuesReturnPreviousOnOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(1, DIST), rw(fmapD1), rw(fmapD2));
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
               map1.eval(key, returnReadWriteGet()).thenAccept(v -> {
                     assertFalse(r.isPresent());
                     assertEquals("one", v);
                  }
               )
         )
      );
   }


   static final class SetStringConstantReturnPrevious<K>
         implements Function<ReadWriteEntryView<K, String>, Optional<String>> {
      @Override
      public Optional<String> apply(ReadWriteEntryView<K, String> rw) {
         Optional<String> prev = rw.find();
         rw.set("one");
         return prev;
      }

      @SuppressWarnings("unchecked")
      @ProtoFactory
      static <K> SetStringConstantReturnPrevious<K> getInstance() {
         return INSTANCE;
      }

      private static final SetStringConstantReturnPrevious INSTANCE = new SetStringConstantReturnPrevious<>();
   }

   public void testSimpleReadWriteForConditionalParamBasedReplace() {
      checkSimpleCacheAvailable();
      assumeNonTransactional();
      // Data does not replicate between simple caches.
      doReadWriteForConditionalParamBasedReplace(supplyIntKey(), rw(fmapS1), rw(fmapS2));
   }

   // Transactions use SimpleClusteredVersions, not NumericVersions, and user is not supposed to modify those
   public void testLocalReadWriteForConditionalParamBasedReplace() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyIntKey(), rw(fmapL1), rw(fmapL2));
   }

   public void testReplReadWriteForConditionalParamBasedReplaceOnNonOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(0, REPL), rw(fmapR1), rw(fmapR2));
   }

   public void testReplReadWriteForConditionalParamBasedReplaceOnOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(1, REPL), rw(fmapR1), rw(fmapR2));
   }

   public void testDistReadWriteForConditionalParamBasedReplaceOnNonOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(0, DIST), rw(fmapD1), rw(fmapD2));
   }

   public void testDistReadWriteForConditionalParamBasedReplaceOnOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(1, DIST), rw(fmapD1), rw(fmapD2));
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
   <K> void doReadWriteForConditionalParamBasedReplace(Supplier<K> keySupplier,
                                                       ReadWriteMap<K, String> map1, ReadWriteMap<K, String> map2) {
      replaceWithVersion(keySupplier, map1, map2, 100, rw -> {
            assertEquals("uno", rw.get());
            assertEquals(Optional.of(new MetaEntryVersion(new NumericVersion(200))),
               rw.findMetaParam(MetaEntryVersion.class));
         }
      );
      replaceWithVersion(keySupplier, map1, map2, 900, rw -> {
         assertEquals(Optional.of("one"), rw.find());
         assertEquals(Optional.of(new MetaEntryVersion(new NumericVersion(100))),
            rw.findMetaParam(MetaEntryVersion.class));
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

   static final class SetStringAndVersionConstant<K>
         implements Function<ReadWriteEntryView<K, String>, Void> {
      @Override
      public Void apply(ReadWriteEntryView<K, String> rw) {
         rw.set("one", new MetaEntryVersion(new NumericVersion(100)));
         return null;
      }

      @SuppressWarnings("unchecked")
      @ProtoFactory
      static <K> SetStringAndVersionConstant<K> getInstance() {
         return INSTANCE;
      }

      private static final SetStringAndVersionConstant INSTANCE =
         new SetStringAndVersionConstant<>();
   }

   static final class VersionBasedConditionalReplace<K>
      implements Function<ReadWriteEntryView<K, String>, ReadWriteEntryView<K, String>> {

      @ProtoField(value = 1, defaultValue = "-1")
      final long version;

      @ProtoFactory
      VersionBasedConditionalReplace(long version) {
         this.version = version;
      }

      @Override
      public ReadWriteEntryView<K, String> apply(ReadWriteEntryView<K, String> rw) {
         Optional<MetaEntryVersion> metaParam = rw.findMetaParam(MetaEntryVersion.class);
         metaParam.ifPresent(metaVersion -> {
            if (metaVersion.get().compareTo(new NumericVersion(version)) == EQUAL)
               rw.set("uno", new MetaEntryVersion(new NumericVersion(200)));
         });
         return rw;
      }
   }

   public void testAutoClose() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() throws Exception {
            Configuration configBuilder = new ConfigurationBuilder().build();
            cm.defineConfiguration("read-only", configBuilder);
            AdvancedCache<?, ?> readOnlyCache = getAdvancedCache(cm, "read-only");
            try (ReadOnlyMap<?, ?> ro = ReadOnlyMapImpl.create(FunctionalMapImpl.create(readOnlyCache))) {
               assertNotNull(ro); // No-op, just verify that it implements AutoCloseable
            }
            assertTrue(readOnlyCache.getStatus().isTerminated());

            cm.defineConfiguration("write-only", configBuilder);
            AdvancedCache<?, ?> writeOnlyCache = getAdvancedCache(cm, "write-only");
            try (WriteOnlyMap<?, ?> wo = WriteOnlyMapImpl.create(FunctionalMapImpl.create(writeOnlyCache))) {
               assertNotNull(wo); // No-op, just verify that it implements AutoCloseable
            }
            assertTrue(writeOnlyCache.getStatus().isTerminated());

            cm.defineConfiguration("read-write", configBuilder);
            AdvancedCache<?, ?> readWriteCache = getAdvancedCache(cm, "read-write");
            try (ReadWriteMap<?, ?> rw = ReadWriteMapImpl.create(FunctionalMapImpl.create(readWriteCache))) {
               assertNotNull(rw); // No-op, just verify that it implements AutoCloseable
            }
            assertTrue(readWriteCache.getStatus().isTerminated());
         }
      });
   }

   public void testSimpleReadOnlyEvalManyEmpty() {
      checkSimpleCacheAvailable();
      doReadOnlyEvalManyEmpty(supplyIntKey(), ro(fmapS1));
   }

   public void testLocalReadOnlyEvalManyEmpty() {
      doReadOnlyEvalManyEmpty(supplyIntKey(), ro(fmapL1));
   }

   public void testReplReadOnlyEvalManyEmptyOnNonOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(0, REPL), ro(fmapR1));
   }

   public void testReplReadOnlyEvalManyEmptyOnOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(1, REPL), ro(fmapR1));
   }

   public void testDistReadOnlyEvalManyEmptyOnNonOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(0, DIST), ro(fmapD1));
   }

   public void testDistReadOnlyEvalManyEmptyOnOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(1, DIST), ro(fmapD1));
   }

   private <K> void doReadOnlyEvalManyEmpty(Supplier<K> keySupplier, ReadOnlyMap<K, String> map) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Traversable<ReadEntryView<K, String>> t = map
         .evalMany(new HashSet<>(Arrays.asList(key1, key2, key3)), identity());
      t.forEach(ro -> assertFalse(ro.find().isPresent()));
   }

   public void testSimpleUpdateSubsetAndReturnPrevs() {
      checkSimpleCacheAvailable();
      doUpdateSubsetAndReturnPrevs(supplyIntKey(), ro(fmapS1), wo(fmapS2), rw(fmapS2));
   }

   public void testLocalUpdateSubsetAndReturnPrevs() {
      doUpdateSubsetAndReturnPrevs(supplyIntKey(), ro(fmapL1), wo(fmapL2), rw(fmapL2));
   }

   public void testReplUpdateSubsetAndReturnPrevsOnNonOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(0, REPL), ro(fmapR1), wo(fmapR2), rw(fmapR2));
   }

   public void testReplUpdateSubsetAndReturnPrevsOnOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(1, REPL), ro(fmapR1), wo(fmapR2), rw(fmapR2));
   }

   public void testDistUpdateSubsetAndReturnPrevsOnNonOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(0, DIST), ro(fmapD1), wo(fmapD2), rw(fmapD2));
   }

   public void testDistUpdateSubsetAndReturnPrevsOnOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(1, DIST), ro(fmapD1), wo(fmapD2), rw(fmapD2));
   }

   private <K> void doUpdateSubsetAndReturnPrevs(Supplier<K> keySupplier,
         ReadOnlyMap<K, String> map1, WriteOnlyMap<K, String> map2, ReadWriteMap<K, String> map3) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      await(map2.evalMany(data, setValueConsumer()));
      Traversable<String> currentValues = map1.evalMany(data.keySet(), returnReadOnlyFindOrNull());
      List<String> collectedValues = currentValues.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      Collections.sort(collectedValues);
      List<String> dataValues = new ArrayList<>(data.values());
      Collections.sort(dataValues);
      assertEquals(collectedValues, dataValues);

      Map<K, String> newData = new HashMap<>();
      newData.put(key1, "bat");
      newData.put(key2, "bi");
      newData.put(key3, "hiru");
      Traversable<String> prevTraversable = map3.evalMany(newData, setValueReturnPrevOrNull());
      List<String> collectedPrev = prevTraversable.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      Collections.sort(collectedPrev);
      assertEquals(dataValues, collectedPrev);

      Traversable<String> updatedValues = map1.evalMany(data.keySet(), returnReadOnlyFindOrNull());
      List<String> collectedUpdates = updatedValues.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      Collections.sort(collectedUpdates);
      List<String> newDataValues = new ArrayList<>(newData.values());
      Collections.sort(newDataValues);
      assertEquals(newDataValues, collectedUpdates);
   }

   public void testSimpleReadWriteToRemoveAllAndReturnPrevs() {
      checkSimpleCacheAvailable();
      doReadWriteToRemoveAllAndReturnPrevs(supplyIntKey(), wo(fmapS1), rw(fmapS2));
   }

   public void testLocalReadWriteToRemoveAllAndReturnPrevs() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyIntKey(), wo(fmapL1), rw(fmapL2));
   }

   public void testReplReadWriteToRemoveAllAndReturnPrevsOnNonOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(0, REPL), wo(fmapR1), rw(fmapR2));
   }

   public void testReplReadWriteToRemoveAllAndReturnPrevsOnOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(1, REPL), wo(fmapR1), rw(fmapR2));
   }

   public void testDistReadWriteToRemoveAllAndReturnPrevsOnNonOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(0, DIST), wo(fmapD1), rw(fmapD2));
   }

   public void testDistReadWriteToRemoveAllAndReturnPrevsOnOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(1, DIST), wo(fmapD1), rw(fmapD2));
   }

   <K> void doReadWriteToRemoveAllAndReturnPrevs(Supplier<K> keySupplier,
                                                 WriteOnlyMap<K, String> map1, ReadWriteMap<K, String> map2) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Map<K, String> data = new HashMap<>();
      data.put(key1, "one");
      data.put(key2, "two");
      data.put(key3, "three");
      await(map1.evalMany(data, setValueConsumer()));
      Traversable<String> prevTraversable = map2.evalAll(removeReturnPrevOrNull());
      Set<String> prevValues = prevTraversable.collect(HashSet::new, HashSet::add, HashSet::addAll);
      assertEquals(new HashSet<>(data.values()), prevValues);
   }

   public void testSimpleReturnViewFromReadOnlyEval() {
      checkSimpleCacheAvailable();
      doReturnViewFromReadOnlyEval(supplyIntKey(), ro(fmapS1), wo(fmapS2));
   }

   public void testLocalReturnViewFromReadOnlyEval() {
      doReturnViewFromReadOnlyEval(supplyIntKey(), ro(fmapL1), wo(fmapL2));
   }

   public void testReplReturnViewFromReadOnlyEvalOnNonOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(0, REPL), ro(fmapR1), wo(fmapR2));
   }

   public void testReplReturnViewFromReadOnlyEvalOnOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(1, REPL), ro(fmapR1), wo(fmapR2));
   }

   public void testDistReturnViewFromReadOnlyEvalOnNonOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(0, DIST), ro(fmapD1), wo(fmapD2));
   }

   public void testDistReturnViewFromReadOnlyEvalOnOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(1, DIST), ro(fmapD1), wo(fmapD2));
   }

   <K> void doReturnViewFromReadOnlyEval(Supplier<K> keySupplier,
                                         ReadOnlyMap<K, String> ro, WriteOnlyMap<K, String> wo) {
      K k = keySupplier.get();
      assertReadOnlyViewEmpty(k, await(ro.eval(k, identity())));
      await(wo.eval(k, wv -> wv.set("one")));
      assertReadOnlyViewEquals(k, "one", await(ro.eval(k, identity())));
   }

   public void testSimpleReturnViewFromReadWriteEval() {
      checkSimpleCacheAvailable();
      doReturnViewFromReadWriteEval(supplyIntKey(), rw(fmapS1), rw(fmapS2));
   }

   public void testLocalReturnViewFromReadWriteEval() {
      doReturnViewFromReadWriteEval(supplyIntKey(), rw(fmapL1), rw(fmapL2));
   }

   public void testReplReturnViewFromReadWriteEvalOnNonOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(0, REPL), rw(fmapR1), rw(fmapR2));
   }

   public void testReplReturnViewFromReadWriteEvalOnOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(1, REPL), rw(fmapR1), rw(fmapR2));
   }

   public void testDistReturnViewFromReadWriteEvalOnNonOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(0, DIST), rw(fmapD1), rw(fmapD2));
   }

   public void testDistReturnViewFromReadWriteEvalOnOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(1, DIST), rw(fmapD1), rw(fmapD2));
   }

   <K> void doReturnViewFromReadWriteEval(Supplier<K> keySupplier,
                                          ReadWriteMap<K, String> readMap, ReadWriteMap<K, String> writeMap) {
      K k = keySupplier.get();
      assertReadWriteViewEmpty(k, await(readMap.eval(k, returnReadWriteView())));
      assertReadWriteViewEquals(k, "one", await(writeMap.eval(k, setOneReadWrite())));
      assertReadWriteViewEquals(k, "one", await(readMap.eval(k, returnReadWriteView())));
      assertReadWriteViewEquals(k, "uno", await(writeMap.eval(k, "uno", setValueReturnView())));
      assertReadWriteViewEquals(k, "uno", await(readMap.eval(k, returnReadWriteView())));
   }

   private <K> SerializableFunction<ReadWriteEntryView<K, String>, ReadWriteEntryView<K, String>> setOneReadWrite() {
      return rw -> {
            rw.set("one");
            return rw;
         };
   }

   private void assumeNonTransactional() {
      SkipTestNG.skipIf(transactional == Boolean.TRUE,
                        "Transactions use SimpleClusteredVersions, not NumericVersions, and user is not supposed to modify those");
   }


   @ProtoSchema(
         dependsOn = TestDataSCI.class,
         includeClasses = {
               FunctionalMapTest.SetStringConstant.class,
               FunctionalMapTest.SetStringConstantReturnPrevious.class,
               FunctionalMapTest.SetStringAndVersionConstant.class,
               FunctionalMapTest.SetValueAndConstantLifespan.class,
               FunctionalMapTest.VersionBasedConditionalReplace.class
         },
         schemaFileName = "test.core.functional.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.functional",
         service = false
   )
   public interface FunctionalMapSCI extends SerializationContextInitializer {
   }
}
