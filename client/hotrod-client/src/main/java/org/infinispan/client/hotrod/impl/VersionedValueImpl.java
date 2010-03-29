package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.RemoteCache;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class VersionedValueImpl<V> implements RemoteCache.VersionedValue<V> {

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
}
