package org.infinispan.stream.impl;

import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Only notifies a completion listener when the last key for a segment has been found. The last key for a segment
 * is assumed to be the last key seen {@link #valueAdded(Object)} before segments
 * are encountered {@link #accept(Supplier)}. Note that this listener can be used for multiple calls for segments
 * but it will always follow {0 - N} valueAdded invocations and then {0 - 1} accept method invocations. The
 * accept method could be invoked 0 times if all segments are lost on the remote node. Also this invocation chain
 * of valueAdded and accept may be done multiple times if there are multiple nodes such that they outnumber the
 * number of remote publishers created.
 */
class KeyWatchingCompletionListener {
   private AtomicReference<Object> currentKey = new AtomicReference<>();

   private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener;
   private final Map<Object, Supplier<PrimitiveIterator.OfInt>> pendingSegments = new ConcurrentHashMap<>();

   KeyWatchingCompletionListener(Consumer<? super Supplier<PrimitiveIterator.OfInt>> completionListener) {
      this.completionListener = completionListener;
   }

   /**
    * Method invoked for each entry added to the stream passing only the key
    * @param key key of entry added to stream
    */
   public void valueAdded(Object key) {
      currentKey.set(key);
   }

   /**
    * Method to be invoked after all entries have been passed to the stream that belong to these segments
    * @param segments the segments that had all entries passed down
    */
   public void accept(Supplier<PrimitiveIterator.OfInt> segments) {
      Supplier<PrimitiveIterator.OfInt> notifyThese;
      Object key = currentKey.get();
      if (key != null) {
         pendingSegments.put(key, segments);
         // We now try to go back and set current key to null
         if (currentKey.getAndSet(null) == null) {
            // If it was already null that means we returned our key via the iterator below
            // In this case they may or may not have seen the pendingSegments so if they didn't we have to
            // notify ourselves
            notifyThese = pendingSegments.remove(key);
         } else {
            // Means that the iteration will see this
            notifyThese = null;
         }
      } else {
         // This means that we got a set of segments that had no entries in them or the iterator
         // consumed all entries, so just notify right away
         notifyThese = segments;
      }
      if (notifyThese != null) {
         completionListener.accept(notifyThese);
      }
   }

   /**
    * This method is to be invoked on possibly a different thread at any point which states that a key has
    * been iterated upon. This is the signal that if a set of segments is waiting for a key to be iterated upon
    * to notify the iteration
    * @param key the key just returning
    */
   public void valueIterated(Object key) {
      // If we set to null that tells segment completion to just notify above in accept
      if (!currentKey.compareAndSet(key, null)) {
         // Otherwise we have to check if this key was linked to a group of pending segments
         Supplier<PrimitiveIterator.OfInt> segments = pendingSegments.remove(key);
         if (segments != null) {
            completionListener.accept(segments);
         }
      }
   }

   /**
    * Invoked after the iterator has completed iterating upon all entries
    */
   public void completed() {
      // This should always be empty
      assert pendingSegments.isEmpty() : "pendingSegments should be empty but was: " + pendingSegments;
   }
}
