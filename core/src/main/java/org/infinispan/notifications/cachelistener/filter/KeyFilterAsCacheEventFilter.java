package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Set;

/**
 * CacheEventFilter that implements it's filtering solely on the use of the provided KeyFilter
 *
 * @author wburns
 * @since 7.0
 */
public class KeyFilterAsCacheEventFilter<K> implements CacheEventFilter<K, Object>, Serializable {
   private final KeyFilter<? super K> filter;

   public KeyFilterAsCacheEventFilter(KeyFilter<? super K> filter) {
      this.filter = filter;
   }

   @Override
   public boolean accept(K key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      return filter.accept(key);
   }

   public static class Externalizer extends AbstractExternalizer<KeyFilterAsCacheEventFilter> {
      @Override
      public Set<Class<? extends KeyFilterAsCacheEventFilter>> getTypeClasses() {
         return Util.<Class<? extends KeyFilterAsCacheEventFilter>>asSet(KeyFilterAsCacheEventFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, KeyFilterAsCacheEventFilter object) throws IOException {
         output.writeObject(object.filter);
      }

      @Override
      public KeyFilterAsCacheEventFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyFilterAsCacheEventFilter((KeyFilter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_FILTER_AS_CACHE_EVENT_FILTER;
      }
   }
}
