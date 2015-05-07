package org.infinispan.commons;

import java.io.Serializable;

public class KeyValueWithPrevious<K, V> implements Serializable {
   /** The serialVersionUID */
   private static final long serialVersionUID = -7875910676423622104L;

   private final K key;
   private final V value;
   private final V prev;

   public KeyValueWithPrevious(K key, V value, V prev) {
      this.key = key;
      this.value = value;
      this.prev = prev;
   }

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }

   public V getPrev() {
      return prev;
   }

   @Override
   public String toString() {
      return String.format("%s{key=%s, value=%s, prev=%s}",
            getClass().getSimpleName(), key, value, prev);
   }
}
