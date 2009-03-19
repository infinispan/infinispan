package org.horizon.loader.bucket;

import org.horizon.loader.StoredEntry;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Collection;

/**
 * A bucket is where entries are stored.
 */
public final class Bucket implements Externalizable {
   private Map<Object, StoredEntry> entries = new HashMap<Object, StoredEntry>();
   private transient String bucketName;

   public final void addEntry(StoredEntry se) {
      entries.put(se.getKey(), se);
   }

   public final boolean removeEntry(Object key) {
      return entries.remove(key) != null;
   }

   public final StoredEntry getEntry(Object key) {
      return entries.get(key);
   }

   public final void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(entries.size());
      for (StoredEntry se : entries.values()) out.writeObject(se);
   }

   public final void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      int sz = in.readInt();
      entries = new HashMap<Object, StoredEntry>(sz);
      for (int i = 0; i < sz; i++) {
         StoredEntry se = (StoredEntry) in.readObject();
         entries.put(se.getKey(), se);
      }
   }

   public Map<Object, StoredEntry> getEntries() {
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
      Iterator<Map.Entry<Object, StoredEntry>> entryIterator = entries.entrySet().iterator();
      while (entryIterator.hasNext()) {
         Map.Entry<Object, StoredEntry> entry = entryIterator.next();
         if (entry.getValue().isExpired()) {
            entryIterator.remove();
            result = true;
         }
      }
      return result;
   }

   public Collection<? extends StoredEntry> getStoredEntries() {
      return entries.values();
   }

   public long timestampOfFirstEntryToExpire() {
      long result = Long.MAX_VALUE;
      for (StoredEntry se : entries.values()) {
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
