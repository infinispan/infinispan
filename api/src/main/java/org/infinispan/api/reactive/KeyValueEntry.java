package org.infinispan.api.reactive;

import java.util.Objects;

public class KeyValueEntry<K, V> {
   private final K key;
   private final V value;
   private final EntryStatus status;

   public KeyValueEntry(K key, V value, EntryStatus status) {
      this.key = key;
      this.value = value;
      this.status = status;
   }

   public K key() {
      return key;
   }

   public V value() {
      return value;
   }

   public EntryStatus status() {
      return status;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      KeyValueEntry<?, ?> that = (KeyValueEntry<?, ?>) o;
      return Objects.equals(key, that.key) &&
            Objects.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      return Objects.hash(key, value);
   }

   @Override
   public String toString() {
      return "KeyValueEntry{" +
            "key=" + key +
            ", value=" + value +
            ", status=" + status +
            '}';
   }
}
