package org.infinispan.loaders.modifications;

import org.infinispan.container.entries.InternalCacheEntry;

/**
 * Modification representing {@link org.infinispan.loaders.CacheStore#store(org.infinispan.container.entries.InternalCacheEntry)}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Store implements Modification {

   final InternalCacheEntry storedEntry;

   public Store(InternalCacheEntry storedEntry) {
      this.storedEntry = storedEntry;
   }

   @Override
   public Type getType() {
      return Type.STORE;
   }

   public InternalCacheEntry getStoredEntry() {
      return storedEntry;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Store store = (Store) o;

      if (storedEntry != null ? !storedEntry.equals(store.storedEntry) : store.storedEntry != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return storedEntry != null ? storedEntry.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "Store{" +
            "storedEntry=" + storedEntry +
            '}';
   }
}
