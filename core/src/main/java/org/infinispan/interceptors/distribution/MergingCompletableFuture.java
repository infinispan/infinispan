package org.infinispan.interceptors.distribution;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.infinispan.statetransfer.OutdatedTopologyException;

class MergingCompletableFuture<T> extends CountDownCompletableFuture {
   private final Function<T[], Object> transform;
   protected final T[] results;
   protected volatile boolean hasUnsureResponse;
   protected volatile boolean lostData;

   MergingCompletableFuture(int participants, T[] results, Function<T[], Object> transform) {
      super(participants);
      // results can be null if the command has flag IGNORE_RETURN_VALUE
      this.results = results;
      this.transform = transform;
   }

   static BiConsumer<MergingCompletableFuture<Object>, Object> moveListItemsToFuture(int myOffset) {
      return (f, rv) -> moveListItemsToFuture(rv, f, myOffset);
   }

   static void moveListItemsToFuture(Object rv, MergingCompletableFuture<Object> f, int myOffset) {
      Collection<?> items;
      if (rv == null && f.results == null) {
         return;
      } else if (rv instanceof Map) {
         items = ((Map) rv).entrySet();
      } else if (rv instanceof Collection) {
         items = (Collection<?>) rv;
      } else if (rv != null && rv.getClass().isArray() && !rv.getClass().getComponentType().isPrimitive()) {
         System.arraycopy(rv, 0, f.results, myOffset, Array.getLength(rv));
         return;
      } else {
         f.completeExceptionally(new IllegalArgumentException("Unexpected result value " + rv));
         return;
      }
      Iterator<?> it = items.iterator();
      for (int i = 0; it.hasNext(); ++i) {
         f.results[myOffset + i] = it.next();
      }
   }

   @Override
   protected Object result() {
      // If we've lost data but did not get any unsure responses we should return limited stream.
      // If we've got unsure response but did not lose any data - no problem, there has been another
      // response delivering the results.
      // Only if those two combine we'll rather throw OTE and retry.
      if (hasUnsureResponse && lostData) {
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }
      return transform == null || results == null ? null : transform.apply(results);
   }
}
