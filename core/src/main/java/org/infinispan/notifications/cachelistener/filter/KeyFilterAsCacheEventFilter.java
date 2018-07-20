package org.infinispan.notifications.cachelistener.filter;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * CacheEventFilter that implements it's filtering solely on the use of the provided KeyFilter
 *
 * @author wburns
 * @since 7.0
 */
public class KeyFilterAsCacheEventFilter<K> implements CacheEventFilter<K, Object> {
   private final KeyFilter<? super K> filter;

   public KeyFilterAsCacheEventFilter(KeyFilter<? super K> filter) {
      this.filter = Objects.requireNonNull(filter);
   }

   @Override
   public boolean accept(K key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      return filter.accept(key);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(filter);
   }

   public static class Externalizer extends AbstractExternalizer<KeyFilterAsCacheEventFilter> {
      @Override
      public Set<Class<? extends KeyFilterAsCacheEventFilter>> getTypeClasses() {
         return Collections.singleton(KeyFilterAsCacheEventFilter.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, KeyFilterAsCacheEventFilter object) throws IOException {
         output.writeObject(object.filter);
      }

      @Override
      public KeyFilterAsCacheEventFilter readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyFilterAsCacheEventFilter((KeyFilter)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_FILTER_AS_CACHE_EVENT_FILTER;
      }
   }
}
