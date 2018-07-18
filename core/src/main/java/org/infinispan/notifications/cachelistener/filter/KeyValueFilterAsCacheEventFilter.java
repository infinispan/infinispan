package org.infinispan.notifications.cachelistener.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * CacheEventFilter that implements it's filtering solely on the use of the provided KeyValueFilter
 *
 * @author wburns
 * @since 7.0
 */
public class KeyValueFilterAsCacheEventFilter<K, V> implements CacheEventFilter<K, V> {
   private final KeyValueFilter<? super K, ? super V> filter;

   public KeyValueFilterAsCacheEventFilter(KeyValueFilter<? super K, ? super V> filter) {
      this.filter = filter;
   }

   @Override
   public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return filter.accept(key, newValue, newMetadata);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(filter);
   }

   public static class Externalizer extends AbstractExternalizer<KeyValueFilterAsCacheEventFilter> {
      @Override
      public Set<Class<? extends KeyValueFilterAsCacheEventFilter>> getTypeClasses() {
         return Collections.singleton(KeyValueFilterAsCacheEventFilter.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, KeyValueFilterAsCacheEventFilter object) throws IOException {
         output.writeObject(object.filter);
      }

      @Override
      public KeyValueFilterAsCacheEventFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyValueFilterAsCacheEventFilter((KeyValueFilter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_VALUE_FILTER_AS_CACHE_EVENT_FILTER;
      }
   }
}
