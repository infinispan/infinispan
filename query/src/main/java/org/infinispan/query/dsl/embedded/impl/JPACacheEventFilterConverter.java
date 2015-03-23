package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class JPACacheEventFilterConverter<K, V, C> extends AbstractCacheEventFilterConverter<K, V, C> implements IndexedFilter<K, V, C> {

   protected final JPAFilterAndConverter<K, V> filterAndConverter;

   public JPACacheEventFilterConverter(JPAFilterAndConverter<K, V> filterAndConverter) {
      this.filterAndConverter = filterAndConverter;
   }

   @Inject
   protected void injectDependencies(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(filterAndConverter);
   }

   @Override
   public C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return (C) filterAndConverter.filterAndConvert(key, newValue, newMetadata);
   }

   public static final class Externalizer extends AbstractExternalizer<JPACacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPACacheEventFilterConverter object) throws IOException {
         output.writeObject(object.filterAndConverter);
      }

      @Override
      public JPACacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         JPAFilterAndConverter filterAndConverter = (JPAFilterAndConverter) input.readObject();
         return new JPACacheEventFilterConverter(filterAndConverter);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPACacheEventFilterConverter>> getTypeClasses() {
         return Collections.<Class<? extends JPACacheEventFilterConverter>>singleton(JPACacheEventFilterConverter.class);
      }
   }
}
