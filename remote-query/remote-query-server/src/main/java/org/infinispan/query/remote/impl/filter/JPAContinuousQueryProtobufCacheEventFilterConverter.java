package org.infinispan.query.remote.impl.filter;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.continuous.impl.JPAContinuousQueryCacheEventFilterConverter;
import org.infinispan.query.remote.client.ContinuousQueryResult;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class JPAContinuousQueryProtobufCacheEventFilterConverter extends JPAContinuousQueryCacheEventFilterConverter<Object, Object, byte[]> {

   private SerializationContext serCtx;

   private boolean usesValueWrapper;

   public JPAContinuousQueryProtobufCacheEventFilterConverter(String jpaQuery, Map<String, Object> namedParameters, Class<? extends Matcher> matcherImplClass) {
      super(jpaQuery, namedParameters, matcherImplClass);
   }

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(EmbeddedCacheManager cacheManager, Configuration cfg) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
      usesValueWrapper = cfg.indexing().index().isEnabled() && !cfg.compatibility().enabled();
   }

   @Override
   public byte[] filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
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
      ContinuousQueryResult result;
      if (f1 == null && f2 != null) {
         result = new ContinuousQueryResult(true, (byte[]) key, f2.getProjection() == null ? (byte[]) newValue : null, f2.getProjection());
      } else if (f1 != null && f2 == null) {
         result = new ContinuousQueryResult(false, (byte[]) key, null, null);
      } else {
         return null;
      }
      try {
         return ProtobufUtil.toByteArray(serCtx, result);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public String toString() {
      return "JPAContinuousQueryProtobufCacheEventFilterConverter{jpaQuery='" + jpaQuery + "'}";
   }

   public static final class Externalizer extends AbstractExternalizer<JPAContinuousQueryProtobufCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAContinuousQueryProtobufCacheEventFilterConverter filterAndConverter) throws IOException {
         output.writeUTF(filterAndConverter.jpaQuery);
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
         String jpaQuery = input.readUTF();
         int paramsSize = UnsignedNumeric.readUnsignedInt(input);
         Map<String, Object> namedParameters = null;
         if (paramsSize != 0) {
            namedParameters = new HashMap<String, Object>(paramsSize);
            for (int i = 0; i < paramsSize; i++) {
               String paramName = input.readUTF();
               Object paramValue = input.readObject();
               namedParameters.put(paramName, paramValue);
            }
         }
         Class<? extends Matcher> matcherImplClass = (Class<? extends Matcher>) input.readObject();
         return new JPAContinuousQueryProtobufCacheEventFilterConverter(jpaQuery, namedParameters, matcherImplClass);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAContinuousQueryProtobufCacheEventFilterConverter>> getTypeClasses() {
         return Collections.<Class<? extends JPAContinuousQueryProtobufCacheEventFilterConverter>>singleton(JPAContinuousQueryProtobufCacheEventFilterConverter.class);
      }
   }
}
