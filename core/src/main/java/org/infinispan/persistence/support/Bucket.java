package org.infinispan.persistence.support;

import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.util.TimeService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A bucket is where entries are stored.
 */
public final class Bucket {
   final Map<Object, MarshalledEntry> entries;
   private transient Integer bucketId;
   private transient String bucketIdStr;


   public Bucket(Equivalence<Object> keyEquivalence) {
      this.entries = CollectionFactory.makeMap(32, keyEquivalence, AnyEquivalence.<MarshalledEntry>getInstance());
   }

   public Bucket(Map<Object, MarshalledEntry> entries, Equivalence<Object> keyEquivalence) {
      this.entries = CollectionFactory.makeMap(entries, keyEquivalence, AnyEquivalence.<MarshalledEntry>getInstance());
   }

   public final void addEntry(Object key,MarshalledEntry sv) {
      entries.put(key, sv);
   }

   public final boolean removeEntry(Object key) {
      return entries.remove(key) != null;
   }

   public final MarshalledEntry getEntry(Object key, TimeService timeService) {
      MarshalledEntry marshalledEntry = entries.get(key);
      if (marshalledEntry == null)
         return null;
      if (marshalledEntry.getMetadata() != null && marshalledEntry.getMetadata().isExpired(timeService.wallClockTime())) {
         return null;
      }
      return marshalledEntry;
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

   public Set<Object> removeExpiredEntries(TimeService timeService) {
      Set<Object> result = new HashSet<Object>();
      long currentTimeMillis = 0;
      Iterator<Map.Entry<Object, MarshalledEntry>> entryIterator = entries.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<Object, MarshalledEntry> entry = entryIterator.next();
         final MarshalledEntry value = entry.getValue();
         if (value.getMetadata() != null) {
            if (currentTimeMillis == 0)
               currentTimeMillis = timeService.wallClockTime();
            if (value.getMetadata().isExpired(currentTimeMillis)) {
               result.add(entry.getKey());
               entryIterator.remove();
            }
         }
      }
      return result;
   }

   public Map<Object, MarshalledEntry> getStoredEntries() {
      return entries;
   }

   public Map<Object, MarshalledEntry> getStoredEntries(AdvancedCacheLoader.KeyFilter filter, TimeService timeService) {
      filter = PersistenceUtil.notNull(filter);
      long currentTimeMillis = timeService.wallClockTime();
      Map<Object, MarshalledEntry> result = new HashMap<Object, MarshalledEntry>();
      for (Map.Entry<Object, MarshalledEntry> entry : getStoredEntries().entrySet()) {
         MarshalledEntry me = entry.getValue();
         if (!isExpired(currentTimeMillis, me) && filter.shouldLoadKey(entry.getKey()))
            result.put(entry.getKey(), me);
      }
      return result;
   }

   private boolean isExpired(long currentTimeMillis, MarshalledEntry me) {
      return me != null && me.getMetadata() != null && me.getMetadata().isExpired(currentTimeMillis);
   }

   public long timestampOfFirstEntryToExpire() {
      long result = Long.MAX_VALUE;
      for (MarshalledEntry se : entries.values()) {
         if (se.getMetadata() != null && se.getMetadata().expiryTime() < result) {
            result = se.getMetadata().expiryTime();
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

   public boolean contains(Object key, TimeService timeService) {
      return getEntry(key, timeService) != null;
   }

   // Bucket externalizer has been removed because it's no longer marshallable.
   // The reason for this is cos the bucket's entry collection must take
   // into account cache-level configured equivalence instances, and passing
   // this in to an externalizer, which is a cache manager level abstraction
   // complicated things a lot. Instead, bucket's entries are now marshalled
   // separately and since that's the only thing that the bucket marshalled,
   // it's a pretty small change.
}
