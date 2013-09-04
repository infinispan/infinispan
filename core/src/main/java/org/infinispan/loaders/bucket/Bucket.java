package org.infinispan.loaders.bucket;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.TimeService;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * A bucket is where entries are stored.
 */
public final class Bucket {
   final Map<Object, InternalCacheEntry> entries;
   private transient Integer bucketId;
   private transient String bucketIdStr;
   private final TimeService timeService;

   public Bucket(TimeService timeService, Equivalence<Object> keyEquivalence) {
      this.timeService = timeService;
      this.entries = CollectionFactory.makeMap(32, keyEquivalence,
            AnyEquivalence.<InternalCacheEntry>getInstance());
   }

   public Bucket(TimeService timeService, Equivalence<Object> keyEquivalence, Map<Object, InternalCacheEntry> entries) {
      this.timeService = timeService;
      this.entries = CollectionFactory.makeMap(entries, keyEquivalence,
            AnyEquivalence.<InternalCacheEntry>getInstance());
   }

   public final void addEntry(InternalCacheEntry se) {
      entries.put(se.getKey(), se);
   }

   public final boolean removeEntry(Object key) {
      return entries.remove(key) != null;
   }

   public final InternalCacheEntry getEntry(Object key) {
      return entries.get(key);
   }

   public Map<Object, InternalCacheEntry> getEntries() {
      return entries;
   }

   public Integer getBucketId() {
      return bucketId;
   }

   public void setBucketId(Integer bucketId) {
      this.bucketId = bucketId;
      bucketIdStr = bucketId.toString();
   }

   public void setBucketId(String bucketId) {
      try {
         setBucketId(Integer.parseInt(bucketId));
      } catch (NumberFormatException e) {
         throw new IllegalArgumentException(
               "bucketId: " + bucketId + " (expected: integer)");
      }
   }

   public String getBucketIdAsString() {
      return bucketIdStr;
   }

   public boolean removeExpiredEntries() {
      boolean result = false;
      long currentTimeMillis = 0;
      Iterator<Map.Entry<Object, InternalCacheEntry>> entryIterator = entries.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<Object, InternalCacheEntry> entry = entryIterator.next();
         final InternalCacheEntry value = entry.getValue();
         if (value.canExpire()) {
            if (currentTimeMillis == 0)
               currentTimeMillis = timeService.wallClockTime();
            if (entry.getValue().isExpired(currentTimeMillis)) {
               entryIterator.remove();
               result = true;
            }
         }
      }
      return result;
   }

   public Collection<? extends InternalCacheEntry> getStoredEntries() {
      return entries.values();
   }

   public long timestampOfFirstEntryToExpire() {
      long result = Long.MAX_VALUE;
      for (InternalCacheEntry se : entries.values()) {
         if (se.getExpiryTime() < result) {
            result = se.getExpiryTime();
         }
      }
      return result;
   }

   @Override
   public String toString() {
      return "Bucket{" +
            "entries=" + entries +
            ", bucketId='" + bucketId + '\'' +
            '}';
   }

   public boolean isEmpty() {
      return entries.isEmpty();
   }

   // Bucket externalizer has been removed because it's no longer marshallable.
   // The reason for this is cos the bucket's entry collection must take
   // into account cache-level configured equivalence instances, and passing
   // this in to an externalizer, which is a cache manager level abstraction
   // complicated things a lot. Instead, bucket's entries are now marshalled
   // separately and since that's the only thing that the bucket marshalled,
   // it's a pretty small change.

}
