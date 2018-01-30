package org.infinispan.stream.impl;

import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.ByRef;

/**
 * Only notifies a completion listener when the last key for a segment has been found. The last key for a segment
 * is assumed to be the last key seen {@link KeyWatchingCompletionListener#valueAdded(Object)} before segments
 * are encountered {@link KeyWatchingCompletionListener#segmentsEncountered(Supplier)}.
 */
class KeyWatchingCompletionListener {
   private final AtomicReference<Object> currentKey = new AtomicReference<>();
   private final Map<Object, Supplier<PrimitiveIterator.OfInt>> pendingSegments = new ConcurrentHashMap<>();
   private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener;
   // The next 2 variables are possible assuming that the iterator is not used concurrently. This way we don't
   // have to allocate them on every entry iterated
   private final ByRef<Supplier<PrimitiveIterator.OfInt>> ref = new ByRef<>(null);
   private final BiFunction<Object, Supplier<PrimitiveIterator.OfInt>, Supplier<PrimitiveIterator.OfInt>> iteratorMapping;

   KeyWatchingCompletionListener(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener) {
      this.completionListener = completionListener;
      this.iteratorMapping = (k, v) -> {
         if (v != null) {
            ref.set(v);
         }
         currentKey.compareAndSet(k, null);
         return null;
      };
   }

   public void valueAdded(Object key) {
      currentKey.set(key);
   }

   public void valueIterated(Object key) {
      pendingSegments.compute(key, iteratorMapping);
      Supplier<PrimitiveIterator.OfInt> segments = ref.get();
      if (segments != null) {
         ref.set(null);
         completionListener.accept(segments);
      }
   }

   public void segmentsEncountered(Supplier<PrimitiveIterator.OfInt> segments) {
      // This code assumes that valueAdded and segmentsEncountered are not invoked concurrently and that all values
      // added for a response before the segments are completed.
      // The valueIterated method can be invoked at any point however.
      // See ClusterStreamManagerImpl$ClusterStreamSubscription.sendRequest where the added and segments are called into
      ByRef<Supplier<PrimitiveIterator.OfInt>> segmentsToNotify = new ByRef<>(segments);
      Object key = currentKey.get();
      if (key != null) {
         pendingSegments.compute(key, (k, v) -> {
            // The iterator has consumed all the keys, so there is no reason to wait: just notify of segment
            // completion immediately
            if (currentKey.get() == null) {
               return null;
            }
            // This means we didn't iterate on a key before segments were completed - means it was empty. The
            // valueIterated notifies the completion of non-empty segments, but we need to notify the completion of
            // empty segments here
            segmentsToNotify.set(v);
            return segments;
         });
      }
      Supplier<PrimitiveIterator.OfInt> notifyThese = segmentsToNotify.get();
      if (notifyThese != null) {
         completionListener.accept(notifyThese);
      }
   }

   public void completed() {
      pendingSegments.forEach((k, v) -> completionListener.accept(v));
   }
}
