package org.infinispan.commons.util;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Where is Java 1.6?
 *
 * @author (various)
 * @since 4.0
 */
public class SimpleImmutableEntry<K, V> implements Map.Entry<K, V>, Serializable {
   private static final long serialVersionUID = -6092752114794052323L;

   private final K key;

   private final V value;

   public SimpleImmutableEntry(Entry<K, V> me) {
      key = me.getKey();
      value = me.getValue();
   }

   public SimpleImmutableEntry(K key, V value) {
      this.key = key;
      this.value = value;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public V setValue(V arg0) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean equals(Object o) {
      if (!(o instanceof Map.Entry))
         return false;
      Map.Entry<?, ?> e2 = (Map.Entry<?, ?>) o;
      return (getKey() == null ? e2.getKey() == null : getKey().equals(e2.getKey()))
            && (getValue() == null ? e2.getValue() == null : getValue().equals(e2.getValue()));
   }

   @Override
   public int hashCode() {
      return (getKey() == null ? 0 : getKey().hashCode()) ^
            (getValue() == null ? 0 : getValue().hashCode());
   }

   @Override
   public String toString() {
      return key + "=" + value;
   }
}
