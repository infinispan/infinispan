package org.infinispan.commons.event;

import java.io.Serializable;

public class KVPEvent<K, V> implements Serializable {
   /** The serialVersionUID */
   private static final long serialVersionUID = -7875910676423622104L;

   private final K key;
   private final V value;
   private final V prev;

   public KVPEvent(K key, V value, V prev) {
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
}
