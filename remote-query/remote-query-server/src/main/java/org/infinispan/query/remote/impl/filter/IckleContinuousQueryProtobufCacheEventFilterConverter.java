package org.infinispan.query.remote.impl.filter;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.impl.continuous.IckleContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.remote.client.impl.ContinuousQueryResult;
import org.infinispan.query.remote.impl.RemoteQueryManager;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_ICKLE_CONTINOUS_QUERY_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER)
public final class IckleContinuousQueryProtobufCacheEventFilterConverter extends IckleContinuousQueryCacheEventFilterConverter<Object, Object, Object> {

   private RemoteQueryManager remoteQueryManager;

   IckleContinuousQueryProtobufCacheEventFilterConverter(String queryString, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      super(queryString, namedParameters, matcherImplClass);
   }

   @ProtoFactory
   IckleContinuousQueryProtobufCacheEventFilterConverter(String queryString, MarshallableMap<String, Object> wrappedNamedParameters,
                                                           Class<? extends Matcher> matcherImplClass) {
      super(queryString, wrappedNamedParameters, matcherImplClass);
   }

   @Override
   protected void injectDependencies(Cache<?, ?> cache) {
      remoteQueryManager = ComponentRegistry.componentOf(cache, RemoteQueryManager.class);
      matcherImplClass = remoteQueryManager.getMatcherClass(APPLICATION_PROTOSTREAM);
      super.injectDependencies(cache);
   }

   @Override
   public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      if (eventType.isExpired()) {
         oldValue = newValue;   // expired events have the expired value as newValue
         newValue = null;
      }

      ObjectFilter objectFilter = getObjectFilter();
      ObjectFilter.FilterResult f1 = oldValue == null ? null : objectFilter.filter(key, oldValue, oldMetadata);
      ObjectFilter.FilterResult f2 = newValue == null ? null : objectFilter.filter(key, newValue, newMetadata);
      if (f1 == null && f2 != null) {
         // result joining
         return makeFilterResult(ContinuousQueryResult.ResultType.JOINING, key, f2.getProjection() == null ? newValue : null, f2.getProjection());
      } else if (f1 != null && f2 == null) {
         // result leaving
         return makeFilterResult(ContinuousQueryResult.ResultType.LEAVING, key, null, null);
      } else {
         return null;
      }
   }

   private Object makeFilterResult(ContinuousQueryResult.ResultType resultType, Object key, Object value, Object[] projection) {
      key = remoteQueryManager.convertKey(key, MediaType.APPLICATION_PROTOSTREAM);
      if (value != null) {
         value = remoteQueryManager.convertValue(value, MediaType.APPLICATION_PROTOSTREAM);
      }
      ContinuousQueryResult result = new ContinuousQueryResult(resultType, (byte[]) key, (byte[]) value, projection);
      return remoteQueryManager.encodeFilterResult(result);
   }

   @Override
   public String toString() {
      return "IckleContinuousQueryProtobufCacheEventFilterConverter{queryString='" + queryString + "'}";
   }
}
