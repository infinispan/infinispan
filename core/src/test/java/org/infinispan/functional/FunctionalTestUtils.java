package org.infinispan.functional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

public final class FunctionalTestUtils {

   public static final int MAX_WAIT_SECS = 30;
   private static final Random R = new Random();

   public static <K> FunctionalMap.ReadOnlyMap<K, String> ro(FunctionalMapImpl<K, String> fmap) {
      return ReadOnlyMapImpl.create(fmap);
   }

   public static <K> FunctionalMap.WriteOnlyMap<K, String> wo(FunctionalMapImpl<K, String> fmap) {
      return WriteOnlyMapImpl.create(fmap);
   }

   public static <K> FunctionalMap.ReadWriteMap<K, String> rw(FunctionalMapImpl<K, String> fmap) {
      return ReadWriteMapImpl.create(fmap);
   }

   public static <K, V> FunctionalMap.ReadOnlyMap<K, V> ro(AdvancedCache<K, V> cache) {
      FunctionalMapImpl<K, V> impl = FunctionalMapImpl.create(cache);
      return ReadOnlyMapImpl.create(impl);
   }

   public static <K, V> FunctionalMap.ReadWriteMap<K, V> rw(Cache<K, V> cache) {
      FunctionalMapImpl<K, V> impl = FunctionalMapImpl.create(cache.getAdvancedCache());
      return ReadWriteMapImpl.create(impl);
   }

   public static <K, V> FunctionalMap.WriteOnlyMap<K, V> wo(Cache<K, V> cache) {
      FunctionalMapImpl<K, V> impl = FunctionalMapImpl.create(cache.getAdvancedCache());
      return WriteOnlyMapImpl.create(impl);
   }

   static Supplier<Integer> supplyIntKey() {
      return () -> R.nextInt(Integer.MAX_VALUE);
   }

   public static <T> T await(CompletableFuture<T> cf) {
      return CompletionStages.join(cf);
   }

   public static <T> T await(CompletionStage<T> cf) {
      try {
         return cf.toCompletableFuture().get(MAX_WAIT_SECS, TimeUnit.SECONDS);
      } catch (TimeoutException | InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   public static <T> List<T> await(List<CompletableFuture<T>> cf) {
      return await(CompletableFutures.sequence(cf));
   }

   public static <K> void assertReadOnlyViewEmpty(K k, ReadEntryView<K, ?> ro) {
      assertEquals(k, ro.key());
      assertFalse(ro.find().isPresent());
   }

   public static <K, V> void assertReadOnlyViewEquals(K k, V expected, ReadEntryView<K, V> ro) {
      assertEquals(k, ro.key());
      assertTrue(ro.find().isPresent());
      assertEquals(expected, ro.find().get());
      assertEquals(expected, ro.get());
   }

   public static <K> void assertReadWriteViewEmpty(K k, ReadWriteEntryView<K, ?> rw) {
      assertEquals(k, rw.key());
      assertFalse(rw.find().isPresent());
   }

   public static <K, V> void assertReadWriteViewEquals(K k, V expected, ReadWriteEntryView<K, V> rw) {
      assertEquals(k, rw.key());
      assertTrue(rw.find().isPresent());
      assertEquals(expected, rw.find().get());
      assertEquals(expected, rw.get());
      try {
         rw.set(null);
         fail("Expected IllegalStateException since entry view cannot be modified outside lambda");
      } catch (IllegalStateException e) {
         // Expected
      }
   }

   private FunctionalTestUtils() {
      // Do not instantiate
   }
}
