package org.infinispan.loaders.bucket;

import org.infinispan.container.entries.InternalCacheEntry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A bucket is where entries are stored.
 */
public final class Bucket implements Externalizable {
   private Map<Object, InternalCacheEntry> entries = new HashMap<Object, InternalCacheEntry>();
   private transient String bucketName;

   public final void addEntry(InternalCacheEntry se) {
      entries.put(se.getKey(), se);
   }

   public final boolean removeEntry(Object key) {
      return entries.remove(key) != null;
   }

   public final InternalCacheEntry getEntry(Object key) {
      return entries.get(key);
   }

   public final void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(entries.size());
      for (InternalCacheEntry se : entries.values()) out.writeObject(se);
   }

   public final void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int sz = in.readInt();
      entries = new HashMap<Object, InternalCacheEntry>(sz);
      for (int i = 0; i < sz; i++) {
         InternalCacheEntry se = (InternalCacheEntry) in.readObject();
         entries.put(se.getKey(), se);
      }
   }

   public Map<Object, InternalCacheEntry> getEntries() {
      return entries;
   }

   public String getBucketName() {
      return bucketName;
   }

   public void setBucketName(String bucketName) {
      this.bucketName = bucketName;
   }

   public boolean removeExpiredEntries() {
      boolean result = false;
      Iterator<Map.Entry<Object, InternalCacheEntry>> entryIterator = entries.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<Object, InternalCacheEntry> entry = entryIterator.next();
         if (entry.getValue().isExpired()) {
            entryIterator.remove();
            result = true;
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
            ", bucketName='" + bucketName + '\'' +
            '}';
   }

   public boolean isEmpty() {
      return entries.isEmpty();
   }
}
