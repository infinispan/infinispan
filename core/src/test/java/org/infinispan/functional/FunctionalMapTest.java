package org.infinispan.functional;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.functional.EntryVersion.NumericEntryVersion;
import org.infinispan.commons.api.functional.EntryView.ReadEntryView;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadOnlyMap;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.commons.api.functional.MetaParam.EntryVersionParam;
import org.infinispan.commons.api.functional.MetaParam.Lifespan;
import org.infinispan.commons.api.functional.Traversable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.infinispan.commons.api.functional.EntryVersion.CompareResult.EQUAL;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.*;

/**
 * Test suite for verifying basic functional map functionality,
 * and for testing out functionality that is not available via standard
 * {@link java.util.concurrent.ConcurrentMap} nor JSR-107 JCache
 * APIs, such as atomic conditional metadata-based replace operations, which
 * are required by Hot Rod.
 */
@Test(groups = "functional", testName = "functional.FunctionalMapTest")
public class FunctionalMapTest extends SingleCacheManagerTest {

   private ReadOnlyMap<Integer, String> readOnlyMap;
   private WriteOnlyMap<Integer, String> writeOnlyMap;
   private ReadWriteMap<Integer, String> readWriteMap;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      AdvancedCache<Integer, String> advCache = cacheManager.<Integer, String>getCache().getAdvancedCache();
      FunctionalMapImpl<Integer, String> functionalMap = FunctionalMapImpl.create(advCache);
      readOnlyMap = ReadOnlyMapImpl.create(functionalMap);
      writeOnlyMap = WriteOnlyMapImpl.create(functionalMap);
      readWriteMap = ReadWriteMapImpl.create(functionalMap);
   }

   /**
    * Read-only allows to retrieve an empty cache entry.
    */
   public void testReadOnlyGetsEmpty() {
      await(readOnlyMap.eval(1, ReadEntryView::find).thenAccept(v -> assertEquals(Optional.empty(), v)));
   }

   /**
    * Write-only allows for constant, non-capturing, values to be written,
    * and read-only allows for those values to be retrieved.
    */
   public void testWriteOnlyNonCapturingConstantValueAndReadOnlyGetsValue() {
      await(
         writeOnlyMap.eval(1, writeView -> writeView.set("one")).thenCompose(r ->
               readOnlyMap.eval(1, ReadEntryView::get).thenAccept(v -> {
                     assertNull(r);
                     assertEquals("one", v);
                  }
               )
         )
      );
   }

   /**
    * Write-only allows for non-capturing values to be written along with metadata,
    * and read-only allows for both values and metadata to be retrieved.
    */
   public void testWriteOnlyNonCapturingValueAndMetadataReadOnlyValueAndMetadata() {
      await(
         writeOnlyMap.eval(1, "one", (v, writeView) -> writeView.set(v, new Lifespan(100000))).thenCompose(r ->
               readOnlyMap.eval(1, ro -> ro).thenAccept(ro -> {
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

   /**
    * Read-write allows to retrieve an empty cache entry.
    */
   public void testReadWriteGetsEmpty() {
      await(readWriteMap.eval(1, ReadWriteEntryView::find).thenAccept(v -> assertEquals(Optional.empty(), v)));
   }

   /**
    * Read-write allows for constant, non-capturing, values to be written,
    * returns previous value, and also allows values to be retrieved.
    */
   public void testReadWriteValuesReturnPreviousAndGet() {
      await(
         readWriteMap.eval(1, readWrite -> {
            Optional<String> prev = readWrite.find();
            readWrite.set("one");
            return prev;
         }).thenCompose(r ->
               readWriteMap.eval(1, ReadWriteEntryView::get).thenAccept(v -> {
                     assertFalse(r.isPresent());
                     assertEquals("one", v);
                  }
               )
         )
      );
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
   public void testReadWriteAllowsForConditionalParameterBasedReplace() {
      replaceWithVersion(100, rw -> {
            assertEquals("uno", rw.get());
            assertEquals(new EntryVersionParam<>(new NumericEntryVersion(200)),
               rw.getMetaParam(EntryVersionParam.ID()));
         }
      );
      replaceWithVersion(900, rw -> {
         assertEquals(Optional.of("one"), rw.find());
         assertEquals(Optional.of(new EntryVersionParam<>(new NumericEntryVersion(100))),
            rw.findMetaParam(EntryVersionParam.ID()));
      });
   }

   private void replaceWithVersion(long version, Consumer<ReadWriteEntryView<Integer, String>> asserts) {
      await(
         readWriteMap.eval(1, rw -> rw.set("one", new EntryVersionParam<>(new NumericEntryVersion(100)))).thenCompose(r ->
               readWriteMap.eval(1, rw -> {
                  EntryVersionParam<Long> versionParam = rw.getMetaParam(EntryVersionParam.ID());
                  if (versionParam.get().compareTo(new NumericEntryVersion(version)) == EQUAL)
                     rw.set("uno", new EntryVersionParam<>(new NumericEntryVersion(200)));
                  return rw;
               }).thenAccept(rw -> {
                     assertNull(r);
                     asserts.accept(rw);
                  }
               )
         )
      );
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

   public void testReadOnlyEvalManyEmpty() {
      Traversable<ReadEntryView<Integer, String>> t = readOnlyMap
         .evalMany(new HashSet<>(Arrays.asList(1, 2, 3)), ro -> ro);
      t.forEach(ro -> assertFalse(ro.find().isPresent()));
   }

   public void testReadWriteManyToUpdateSubsetAndReturnPreviousValues() {
      Map<Integer, String> data = new HashMap<>();
      data.put(1, "one");
      data.put(2, "two");
      data.put(3, "three");
      consume(writeOnlyMap.evalMany(data, (v, wo) -> wo.set(v)));
      Traversable<String> currentValues = readOnlyMap.evalMany(data.keySet(), ro -> ro.get());
      List<String> collectedValues = currentValues.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(collectedValues, new ArrayList<>(data.values()));

      Map<Integer, String> newData = new HashMap<>();
      newData.put(1, "bat");
      newData.put(2, "bi");
      newData.put(3, "hiru");
      Traversable<String> prevTraversable = readWriteMap.evalMany(newData, (v, rw) -> {
         String prev = rw.get();
         rw.set(v);
         return prev;
      });
      List<String> collectedPrev = prevTraversable.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(new ArrayList<>(data.values()), collectedPrev);

      Traversable<String> updatedValues = readOnlyMap.evalMany(data.keySet(), ro -> ro.get());
      List<String> collectedUpdates = updatedValues.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(new ArrayList<>(newData.values()), collectedUpdates);
   }

   public void testReadWriteEntriesToRemoveAllAndReturnPreviousValues() {
      Map<Integer, String> data = new HashMap<>();
      data.put(1, "one");
      data.put(2, "two");
      data.put(3, "three");
      consume(writeOnlyMap.evalMany(data, (v, wo) -> wo.set(v)));
      Traversable<String> prevTraversable = readWriteMap.evalAll(rw -> {
         String prev = rw.get();
         rw.remove();
         return prev;
      });
      List<String> prevValues = prevTraversable.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
      assertEquals(new ArrayList<>(data.values()), prevValues);
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
