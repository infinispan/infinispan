package org.infinispan.stream;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @author wburns
 * @since 8.0
 */
public class NoOpFilterConverterWithDependencies<K, V> extends AbstractKeyValueFilterConverter<K, V, V>
        implements Serializable {

   private transient Cache cache;

   @Inject
   protected void injectDependencies(Cache cache) {
      this.cache = cache;
   }

   @Override
   public V filterAndConvert(K key, V value, Metadata metadata) {
      if (cache == null) {
         throw new IllegalStateException("Dependencies were not injected");
      }
      return value;
   }
}
