package org.infinispan.functional;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;

public final class FunctionalTestUtils {

   static final Random R = new Random();

   static <K> FunctionalMap.ReadOnlyMap<K, String> ro(FunctionalMapImpl<K, String> fmap) {
      return ReadOnlyMapImpl.create(fmap);
   }

   static <K> FunctionalMap.WriteOnlyMap<K, String> wo(FunctionalMapImpl<K, String> fmap) {
      return WriteOnlyMapImpl.create(fmap);
   }

   static <K> FunctionalMap.ReadWriteMap<K, String> rw(FunctionalMapImpl<K, String> fmap) {
      return ReadWriteMapImpl.create(fmap);
   }

   static Supplier<Integer> supplyIntKey() {
      return () -> R.nextInt(Integer.MAX_VALUE);
   }

   public static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
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
