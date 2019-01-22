package org.infinispan.api.collections.reactive;

public class KeyValueEntry<K, V> {
   private final K key;
   private final V value;

   public KeyValueEntry(K key, V value) {

      this.key = key;
      this.value = value;
   }

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }
}
