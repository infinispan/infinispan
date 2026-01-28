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

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;

public final class FunctionalTestUtils {

   public static final int MAX_WAIT_SECS = 30;
   private static final Random R = new Random();

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
