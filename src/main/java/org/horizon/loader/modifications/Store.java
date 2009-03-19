package org.horizon.loader.modifications;

import org.horizon.loader.StoredEntry;

/**
 * Modification representing {@link org.horizon.loader.CacheStore#store(org.horizon.loader.StoredEntry)}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Store implements Modification {

   final StoredEntry storedEntry;

   public Store(StoredEntry storedEntry) {
      this.storedEntry = storedEntry;
   }

   public Type getType() {
      return Type.STORE;
   }

   public StoredEntry getStoredEntry() {
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
      int result = storedEntry != null ? storedEntry.hashCode() : 0;
      return result;
   }

   @Override
   public String toString() {
      return "Store{" +
            "storedEntry=" + storedEntry +
            '}';
   }
}
