package org.infinispan.query.dsl.embedded.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.query.impl.externalizers.ExternalizerIds;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class IckleCacheEventFilterConverter<K, V, C> extends AbstractCacheEventFilterConverter<K, V, C> implements IndexedFilter<K, V, C> {

   protected final IckleFilterAndConverter<K, V> filterAndConverter;

   public IckleCacheEventFilterConverter(IckleFilterAndConverter<K, V> filterAndConverter) {
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

   public static final class Externalizer extends AbstractExternalizer<IckleCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleCacheEventFilterConverter object) throws IOException {
         output.writeObject(object.filterAndConverter);
      }

      @Override
      public IckleCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         IckleFilterAndConverter filterAndConverter = (IckleFilterAndConverter) input.readObject();
         return new IckleCacheEventFilterConverter(filterAndConverter);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(IckleCacheEventFilterConverter.class);
      }
   }
}
