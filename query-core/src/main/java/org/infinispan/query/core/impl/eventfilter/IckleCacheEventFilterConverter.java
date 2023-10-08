package org.infinispan.query.core.impl.eventfilter;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.notifications.cachelistener.filter.IndexedFilter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.ICKLE_CACHE_EVENT_FILTER_CONVERTER)
@Scope(Scopes.NONE)
public class IckleCacheEventFilterConverter<K, V, C> extends AbstractCacheEventFilterConverter<K, V, C> implements IndexedFilter<K, V, C> {

   protected final IckleFilterAndConverter<K, V> filterAndConverter;

   @ProtoFactory
   public IckleCacheEventFilterConverter(IckleFilterAndConverter<K, V> filterAndConverter) {
      this.filterAndConverter = filterAndConverter;
   }

   @ProtoField(1)
   protected IckleFilterAndConverter<K, V> getFilterAndConverter() {
      return filterAndConverter;
   }

   @Inject
   protected void injectDependencies(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(filterAndConverter);
   }

   @Override
   public C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return (C) filterAndConverter.filterAndConvert(key, newValue, newMetadata);
   }
}
