package org.infinispan.persistence;

/**
 * @author Pedro Ruivo
 * @since 11.0
 */
public class IdentityKeyValueWrapper<K, V> implements KeyValueWrapper<K, V, V> {

   private static final IdentityKeyValueWrapper<?, ?> INSTANCE = new IdentityKeyValueWrapper<>();

   private IdentityKeyValueWrapper() {
   }

   public static <K1, V1> KeyValueWrapper<K1, V1, V1> instance() {
      //noinspection unchecked
      return (KeyValueWrapper<K1, V1, V1>) INSTANCE;
   }

   @Override
   public V wrap(K key, V value) {
      return value;
   }

   @Override
   public V unwrap(V value) {
      return value;
   }
}
