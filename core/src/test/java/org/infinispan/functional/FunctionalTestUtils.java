package org.infinispan.functional;

import org.infinispan.commons.api.functional.FunctionalMap;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.functional.impl.WriteOnlyMapImpl;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

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

   static void consume(CloseableIterator<Void> it) {
      while (it.hasNext())
         it.next();
   }

   static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get();
      } catch (InterruptedException | ExecutionException e) {
         throw new Error(e);
      }
   }

   private FunctionalTestUtils() {
      // Do not instantiate
   }

}
