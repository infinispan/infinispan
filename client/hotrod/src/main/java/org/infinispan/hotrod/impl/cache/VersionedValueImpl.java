package org.infinispan.hotrod.impl.cache;

/**
 * @since 14.0
 */
public class VersionedValueImpl<V> implements VersionedValue<V> {

   private long version;

   private V value;

   public VersionedValueImpl(long version, V value) {
      this.version = version;
      this.value = value;
   }

   @Override
   public long getVersion() {
      return version;
   }

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public String toString() {
      return "VersionedValueImpl{" +
            "version=" + version +
            ", value=" + value +
            '}';
   }
}
