package org.infinispan.persistence.modifications;

import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * Modification representing {@link org.infinispan.persistence.spi.NonBlockingStore#write(int, MarshallableEntry)}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Store implements Modification {

   final Object key;
   final MarshallableEntry storedEntry;

   public Store(Object key, MarshallableEntry storedValue) {
      this.key = key;
      this.storedEntry = storedValue;
   }

   @Override
   public Type getType() {
      return Type.STORE;
   }

   public MarshallableEntry getStoredValue() {
      return storedEntry;
   }

   public Object getKey() {
      return key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Store)) return false;

      Store store = (Store) o;

      if (key != null ? !key.equals(store.key) : store.key != null) return false;
      if (storedEntry != null ? !storedEntry.equals(store.storedEntry) : store.storedEntry != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (storedEntry != null ? storedEntry.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "Store{" +
            "key=" + key +
            ", storedEntry=" + storedEntry +
            '}';
   }
}
