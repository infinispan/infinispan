package org.infinispan.query.remote.impl.filter;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.continuous.impl.IckleContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.RemoteQueryManager;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class IckleContinuousQueryProtobufCacheEventFilterConverter extends IckleContinuousQueryCacheEventFilterConverter<Object, Object, Object> {

   private RemoteQueryManager remoteQueryManager;

   IckleContinuousQueryProtobufCacheEventFilterConverter(String queryString, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      super(queryString, namedParameters, matcherImplClass);
   }

   @Override
   protected void injectDependencies(Cache cache) {
      remoteQueryManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RemoteQueryManager.class);
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
      ObjectFilter.FilterResult f1 = oldValue == null ? null : objectFilter.filter(oldValue);
      ObjectFilter.FilterResult f2 = newValue == null ? null : objectFilter.filter(newValue);
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

   public static final class Externalizer extends AbstractExternalizer<IckleContinuousQueryProtobufCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleContinuousQueryProtobufCacheEventFilterConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.queryString);
         Map<String, Object> namedParameters = filterAndConverter.namedParameters;
         if (namedParameters != null) {
            UnsignedNumeric.writeUnsignedInt(output, namedParameters.size());
            for (Map.Entry<String, Object> e : namedParameters.entrySet()) {
               output.writeUTF(e.getKey());
               output.writeObject(e.getValue());
            }
         } else {
            UnsignedNumeric.writeUnsignedInt(output, 0);
         }
         output.writeObject(filterAndConverter.matcherImplClass);
      }

      @Override
      @SuppressWarnings("unchecked")
      public IckleContinuousQueryProtobufCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String queryString = input.readUTF();
         int paramsSize = UnsignedNumeric.readUnsignedInt(input);
         Map<String, Object> namedParameters = null;
         if (paramsSize != 0) {
            namedParameters = new HashMap<>(paramsSize);
            for (int i = 0; i < paramsSize; i++) {
               String paramName = input.readUTF();
               Object paramValue = input.readObject();
               namedParameters.put(paramName, paramValue);
            }
         }
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new IckleContinuousQueryProtobufCacheEventFilterConverter(queryString, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleContinuousQueryProtobufCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(IckleContinuousQueryProtobufCacheEventFilterConverter.class);
      }
   }
}
