package org.infinispan.query.remote.impl.filter;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.impl.eventfilter.IckleCacheEventFilterConverter;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.RemoteQueryManager;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_ICKLE_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER)
public final class IckleProtobufCacheEventFilterConverter extends IckleCacheEventFilterConverter<Object, Object, Object> {

   private RemoteQueryManager remoteQueryManager;

   @ProtoFactory
   IckleProtobufCacheEventFilterConverter(IckleProtobufFilterAndConverter filterAndConverter) {
      super(filterAndConverter);
   }

   @Override
   @ProtoField(1)
   protected IckleProtobufFilterAndConverter getFilterAndConverter() {
      return (IckleProtobufFilterAndConverter) super.getFilterAndConverter();
   }

   @Inject
   void injectDependencies(Cache cache) {
      remoteQueryManager = ComponentRegistry.componentOf(cache, RemoteQueryManager.class);
   }

   @Override
   public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      ObjectFilter.FilterResult filterResult = filterAndConverter.filterAndConvert(key, newValue, newMetadata);
      if (filterResult != null) {
         FilterResult result = new FilterResult(filterResult.getInstance(), filterResult.getProjection(), filterResult.getSortProjection());
         return remoteQueryManager.encodeFilterResult(result);
      }
      return null;
   }
}
