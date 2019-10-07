package org.infinispan.container.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;

public class NonSegmentedUtils {
   private NonSegmentedUtils() { }

   private static final int REMOTE_SEGMENT_BATCH_SIZE = 32;

   public static <K, V> void removeSegmentEntries(DataContainer<K, V> dataContainer, IntSet segments,
         List<Consumer<Iterable<InternalCacheEntry<K, V>>>> listeners,
         KeyPartitioner keyPartitioner) {
      if (!segments.isEmpty()) {
         List<InternalCacheEntry<K, V>> removedEntries;
         if (!listeners.isEmpty()) {
            removedEntries = new ArrayList<>(REMOTE_SEGMENT_BATCH_SIZE);
         } else {
            removedEntries = null;
         }
         Iterator<InternalCacheEntry<K, V>> iter = dataContainer.iteratorIncludingExpired();
         while (iter.hasNext()) {
            InternalCacheEntry<K, V> ice = iter.next();
            if (!segments.contains(keyPartitioner.getSegment(ice.getKey())))
               continue;

            dataContainer.remove(ice.getKey());

            if (removedEntries != null) {
               removedEntries.add(ice);
               if (removedEntries.size() == REMOTE_SEGMENT_BATCH_SIZE) {
                  List<InternalCacheEntry<K, V>> unmod = Collections.unmodifiableList(removedEntries);
                  listeners.forEach(c -> c.accept(unmod));
                  removedEntries.clear();
               }
            }
         }
         if (removedEntries != null && !removedEntries.isEmpty()) {
            List<InternalCacheEntry<K, V>> unmod = Collections.unmodifiableList(removedEntries);
            listeners.forEach(c -> c.accept(unmod));
         }
      }
   }
}
