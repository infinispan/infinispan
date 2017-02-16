package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.continuous.impl.JPAContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.CompatibilityReflectionMatcher;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class JPAContinuousQueryProtobufCacheEventFilterConverter extends JPAContinuousQueryCacheEventFilterConverter<Object, Object, Object> {

   private SerializationContext serCtx;

   private boolean usesValueWrapper;

   private boolean isCompatMode;

   public JPAContinuousQueryProtobufCacheEventFilterConverter(String queryString, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      super(queryString, namedParameters, matcherImplClass);
   }

   @Override
   protected void injectDependencies(Cache cache) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
      Configuration cfg = cache.getCacheConfiguration();
      isCompatMode = cfg.compatibility().enabled();
      usesValueWrapper = cfg.indexing().index().isEnabled() && !isCompatMode;
      if (isCompatMode) {
         matcherImplClass = CompatibilityReflectionMatcher.class;
      }
      super.injectDependencies(cache);
   }

   @Override
   public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      if (usesValueWrapper) {
         oldValue = oldValue != null ? ((ProtobufValueWrapper) oldValue).getBinary() : null;
         newValue = newValue != null ? ((ProtobufValueWrapper) newValue).getBinary() : null;
      }

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

   protected Object makeFilterResult(ContinuousQueryResult.ResultType resultType, Object key, Object value, Object[] projection) {
      try {
         if (isCompatMode) {
            key = ProtobufUtil.toWrappedByteArray(serCtx, key);
            if (value != null) {
               value = ProtobufUtil.toWrappedByteArray(serCtx, value);
            }
         }

         Object result = new ContinuousQueryResult(resultType, (byte[]) key, (byte[]) value, projection);

         if (!isCompatMode) {
            result = ProtobufUtil.toWrappedByteArray(serCtx, result);
         }

         return result;
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public String toString() {
      return "JPAContinuousQueryProtobufCacheEventFilterConverter{queryString='" + queryString + "'}";
   }

   public static final class Externalizer extends AbstractExternalizer<JPAContinuousQueryProtobufCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAContinuousQueryProtobufCacheEventFilterConverter filterAndConverter) throws IOException {
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
      public JPAContinuousQueryProtobufCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
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
         return new JPAContinuousQueryProtobufCacheEventFilterConverter(queryString, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAContinuousQueryProtobufCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(JPAContinuousQueryProtobufCacheEventFilterConverter.class);
      }
   }
}
