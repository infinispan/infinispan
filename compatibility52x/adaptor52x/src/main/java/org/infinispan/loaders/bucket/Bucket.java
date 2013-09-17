package org.infinispan.loaders.bucket;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A bucket is where entries are stored.
 */
public final class Bucket implements Serializable {
   final Map<Object, InternalCacheEntry> entries = new HashMap<Object, InternalCacheEntry>(32);
   private transient Integer bucketId;
   private transient String bucketIdStr;

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
               currentTimeMillis = System.currentTimeMillis();
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

   public int getNumEntries() {
      return entries.size();
   }

   public void clearEntries() {
      entries.clear();
   }

   public static class Externalizer extends AbstractExternalizer<Bucket> {

      private static final long serialVersionUID = -5291318076267612501L;

      @Override
      public void writeObject(ObjectOutput output, Bucket b) throws IOException {
         Map<Object, InternalCacheEntry> entries = b.entries;
         UnsignedNumeric.writeUnsignedInt(output, entries.size());
         for (InternalCacheEntry se : entries.values()) {
            output.writeObject(se);
         }
      }

      @Override
      public Bucket readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Bucket b = new Bucket();
         int numEntries = UnsignedNumeric.readUnsignedInt(input);
         for (int i = 0; i < numEntries; i++) {
            b.addEntry((InternalCacheEntry) input.readObject());
         }
         return b;
      }

      @Override
      public Integer getId() {
         return 42 /** bucket id*/;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends Bucket>> getTypeClasses() {
         return Util.<Class<? extends Bucket>>asSet(Bucket.class);
      }
   }
}
