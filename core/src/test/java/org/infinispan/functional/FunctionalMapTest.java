package org.infinispan.functional;

import static org.infinispan.container.versioning.InequalVersionComparisonResult.EQUAL;
import static org.infinispan.functional.FunctionalTestUtils.assertReadOnlyViewEmpty;
import static org.infinispan.functional.FunctionalTestUtils.assertReadOnlyViewEquals;
import static org.infinispan.functional.FunctionalTestUtils.assertReadWriteViewEmpty;
import static org.infinispan.functional.FunctionalTestUtils.assertReadWriteViewEquals;
import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.infinispan.functional.FunctionalTestUtils.supplyIntKey;
import static org.infinispan.marshall.core.MarshallableFunctions.identity;
import static org.infinispan.marshall.core.MarshallableFunctions.removeReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadOnlyFindOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadWriteFind;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadWriteGet;
import static org.infinispan.marshall.core.MarshallableFunctions.returnReadWriteView;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueConsumer;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueReturnPrevOrNull;
import static org.infinispan.marshall.core.MarshallableFunctions.setValueReturnView;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

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

import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.functional.MetaParam.MetaEntryVersion;
import org.infinispan.functional.MetaParam.MetaLifespan;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
import org.infinispan.test.TestDataSCI;
import org.infinispan.testing.skip.SkipTestNG;
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
      doWriteConstantAndReadGetsValue(supplyIntKey(), fmapS1.toReadOnlyMap(), fmapS2.toWriteOnlyMap());
   }

   public void testLocalWriteConstantAndReadGetsValue() {
      doWriteConstantAndReadGetsValue(supplyIntKey(), fmapL1.toReadOnlyMap(), fmapL2.toWriteOnlyMap());
   }

   public void testReplWriteConstantAndReadGetsValueOnNonOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(0, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap());
   }

   public void testReplWriteConstantAndReadGetsValueOnOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(1, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap());
   }

   public void testDistWriteConstantAndReadGetsValueOnNonOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(0, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap());
   }

   public void testDistWriteConstantAndReadGetsValueOnOwner() {
      doWriteConstantAndReadGetsValue(supplyKeyForCache(1, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap());
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
      doWriteValueAndReadValueAndMetadata(supplyIntKey(), fmapS1.toReadOnlyMap(), fmapS2.toWriteOnlyMap());
   }

   public void testLocalWriteValueAndReadValueAndMetadata() {
      doWriteValueAndReadValueAndMetadata(supplyIntKey(), fmapL1.toReadOnlyMap(), fmapL2.toWriteOnlyMap());
   }

   public void testReplWriteValueAndReadValueAndMetadataOnNonOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(0, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap());
   }

   public void testReplWriteValueAndReadValueAndMetadataOnOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(1, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap());
   }

   public void testDistWriteValueAndReadValueAndMetadataOnNonOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(0, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap());
   }

   public void testDistWriteValueAndReadValueAndMetadataOnOwner() {
      doWriteValueAndReadValueAndMetadata(supplyKeyForCache(1, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap());
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
      doReadWriteGetsEmpty(supplyIntKey(), fmapS1.toReadWriteMap());
   }

   public void testLocalReadWriteGetsEmpty() {
      doReadWriteGetsEmpty(supplyIntKey(), fmapL1.toReadWriteMap());
   }

   public void testReplReadWriteGetsEmptyOnNonOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(0, REPL), fmapR1.toReadWriteMap());
   }

   public void testReplReadWriteGetsEmptyOnOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(1, REPL), fmapR1.toReadWriteMap());
   }

   public void testDistReadWriteGetsEmptyOnNonOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(0, DIST), fmapD1.toReadWriteMap());
   }

   public void testDistReadWriteGetsEmptyOnOwner() {
      doReadWriteGetsEmpty(supplyKeyForCache(1, DIST), fmapD1.toReadWriteMap());
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
      doReadWriteConstantReturnPrev(supplyIntKey(), fmapS1.toReadWriteMap(), fmapS2.toReadWriteMap());
   }

   public void testLocalReadWriteValuesReturnPrevious() {
      doReadWriteConstantReturnPrev(supplyIntKey(), fmapL1.toReadWriteMap(), fmapL2.toReadWriteMap());
   }

   public void testReplReadWriteValuesReturnPreviousOnNonOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(0, REPL), fmapR1.toReadWriteMap(), fmapR2.toReadWriteMap());
   }

   public void testReplReadWriteValuesReturnPreviousOnOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(1, REPL), fmapR1.toReadWriteMap(), fmapR2.toReadWriteMap());
   }

   public void testDistReadWriteValuesReturnPreviousOnNonOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(0, DIST), fmapD1.toReadWriteMap(), fmapD2.toReadWriteMap());
   }

   public void testDistReadWriteValuesReturnPreviousOnOwner() {
      doReadWriteConstantReturnPrev(supplyKeyForCache(1, DIST), fmapD1.toReadWriteMap(), fmapD2.toReadWriteMap());
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
      doReadWriteForConditionalParamBasedReplace(supplyIntKey(), fmapS1.toReadWriteMap(), fmapS2.toReadWriteMap());
   }

   // Transactions use SimpleClusteredVersions, not NumericVersions, and user is not supposed to modify those
   public void testLocalReadWriteForConditionalParamBasedReplace() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyIntKey(), fmapL1.toReadWriteMap(), fmapL2.toReadWriteMap());
   }

   public void testReplReadWriteForConditionalParamBasedReplaceOnNonOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(0, REPL), fmapR1.toReadWriteMap(), fmapR2.toReadWriteMap());
   }

   public void testReplReadWriteForConditionalParamBasedReplaceOnOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(1, REPL), fmapR1.toReadWriteMap(), fmapR2.toReadWriteMap());
   }

   public void testDistReadWriteForConditionalParamBasedReplaceOnNonOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(0, DIST), fmapD1.toReadWriteMap(), fmapD2.toReadWriteMap());
   }

   public void testDistReadWriteForConditionalParamBasedReplaceOnOwner() {
      assumeNonTransactional();
      doReadWriteForConditionalParamBasedReplace(supplyKeyForCache(1, DIST), fmapD1.toReadWriteMap(), fmapD2.toReadWriteMap());
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

      @ProtoField(1)
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

   public void testSimpleReadOnlyEvalManyEmpty() {
      checkSimpleCacheAvailable();
      doReadOnlyEvalManyEmpty(supplyIntKey(), fmapS1.toReadOnlyMap());
   }

   public void testLocalReadOnlyEvalManyEmpty() {
      doReadOnlyEvalManyEmpty(supplyIntKey(), fmapL1.toReadOnlyMap());
   }

   public void testReplReadOnlyEvalManyEmptyOnNonOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(0, REPL), fmapR1.toReadOnlyMap());
   }

   public void testReplReadOnlyEvalManyEmptyOnOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(1, REPL), fmapR1.toReadOnlyMap());
   }

   public void testDistReadOnlyEvalManyEmptyOnNonOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(0, DIST), fmapD1.toReadOnlyMap());
   }

   public void testDistReadOnlyEvalManyEmptyOnOwner() {
      doReadOnlyEvalManyEmpty(supplyKeyForCache(1, DIST), fmapD1.toReadOnlyMap());
   }

   private <K> void doReadOnlyEvalManyEmpty(Supplier<K> keySupplier, ReadOnlyMap<K, String> map) {
      K key1 = keySupplier.get(), key2 = keySupplier.get(), key3 = keySupplier.get();
      Traversable<ReadEntryView<K, String>> t = map
         .evalMany(new HashSet<>(Arrays.asList(key1, key2, key3)), identity());
      t.forEach(ro -> assertFalse(ro.find().isPresent()));
   }

   public void testSimpleUpdateSubsetAndReturnPrevs() {
      checkSimpleCacheAvailable();
      doUpdateSubsetAndReturnPrevs(supplyIntKey(), fmapS1.toReadOnlyMap(), fmapS2.toWriteOnlyMap(), fmapS2.toReadWriteMap());
   }

   public void testLocalUpdateSubsetAndReturnPrevs() {
      doUpdateSubsetAndReturnPrevs(supplyIntKey(), fmapL1.toReadOnlyMap(), fmapL2.toWriteOnlyMap(), fmapL2.toReadWriteMap());
   }

   public void testReplUpdateSubsetAndReturnPrevsOnNonOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(0, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap(), fmapR2.toReadWriteMap());
   }

   public void testReplUpdateSubsetAndReturnPrevsOnOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(1, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap(), fmapR2.toReadWriteMap());
   }

   public void testDistUpdateSubsetAndReturnPrevsOnNonOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(0, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap(), fmapD2.toReadWriteMap());
   }

   public void testDistUpdateSubsetAndReturnPrevsOnOwner() {
      doUpdateSubsetAndReturnPrevs(supplyKeyForCache(1, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap(), fmapD2.toReadWriteMap());
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
      doReadWriteToRemoveAllAndReturnPrevs(supplyIntKey(), fmapS1.toWriteOnlyMap(), fmapS2.toReadWriteMap());
   }

   public void testLocalReadWriteToRemoveAllAndReturnPrevs() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyIntKey(), fmapL1.toWriteOnlyMap(), fmapL2.toReadWriteMap());
   }

   public void testReplReadWriteToRemoveAllAndReturnPrevsOnNonOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(0, REPL), fmapR1.toWriteOnlyMap(), fmapR2.toReadWriteMap());
   }

   public void testReplReadWriteToRemoveAllAndReturnPrevsOnOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(1, REPL), fmapR1.toWriteOnlyMap(), fmapR2.toReadWriteMap());
   }

   public void testDistReadWriteToRemoveAllAndReturnPrevsOnNonOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(0, DIST), fmapD1.toWriteOnlyMap(), fmapD2.toReadWriteMap());
   }

   public void testDistReadWriteToRemoveAllAndReturnPrevsOnOwner() {
      doReadWriteToRemoveAllAndReturnPrevs(supplyKeyForCache(1, DIST), fmapD1.toWriteOnlyMap(), fmapD2.toReadWriteMap());
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
      doReturnViewFromReadOnlyEval(supplyIntKey(), fmapS1.toReadOnlyMap(), fmapS2.toWriteOnlyMap());
   }

   public void testLocalReturnViewFromReadOnlyEval() {
      doReturnViewFromReadOnlyEval(supplyIntKey(), fmapL1.toReadOnlyMap(), fmapL2.toWriteOnlyMap());
   }

   public void testReplReturnViewFromReadOnlyEvalOnNonOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(0, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap());
   }

   public void testReplReturnViewFromReadOnlyEvalOnOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(1, REPL), fmapR1.toReadOnlyMap(), fmapR2.toWriteOnlyMap());
   }

   public void testDistReturnViewFromReadOnlyEvalOnNonOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(0, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap());
   }

   public void testDistReturnViewFromReadOnlyEvalOnOwner() {
      doReturnViewFromReadOnlyEval(supplyKeyForCache(1, DIST), fmapD1.toReadOnlyMap(), fmapD2.toWriteOnlyMap());
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
      doReturnViewFromReadWriteEval(supplyIntKey(), fmapS1.toReadWriteMap(), fmapS2.toReadWriteMap());
   }

   public void testLocalReturnViewFromReadWriteEval() {
      doReturnViewFromReadWriteEval(supplyIntKey(), fmapL1.toReadWriteMap(), fmapL2.toReadWriteMap());
   }

   public void testReplReturnViewFromReadWriteEvalOnNonOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(0, REPL), fmapR1.toReadWriteMap(), fmapR2.toReadWriteMap());
   }

   public void testReplReturnViewFromReadWriteEvalOnOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(1, REPL), fmapR1.toReadWriteMap(), fmapR2.toReadWriteMap());
   }

   public void testDistReturnViewFromReadWriteEvalOnNonOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(0, DIST), fmapD1.toReadWriteMap(), fmapD2.toReadWriteMap());
   }

   public void testDistReturnViewFromReadWriteEvalOnOwner() {
      doReturnViewFromReadWriteEval(supplyKeyForCache(1, DIST), fmapD1.toReadWriteMap(), fmapD2.toReadWriteMap());
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
         schemaFilePath = "org/infinispan",
         schemaPackageName = "org.infinispan.test.core.functional",
         service = false,
         syntax = ProtoSyntax.PROTO3
   )
   public interface FunctionalMapSCI extends SerializationContextInitializer {
   }
}
