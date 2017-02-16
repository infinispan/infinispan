package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.embedded.impl.JPACacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class JPAProtobufCacheEventFilterConverter extends JPACacheEventFilterConverter<Object, Object, Object> {

   private SerializationContext serCtx;

   private boolean isCompatMode;

   public JPAProtobufCacheEventFilterConverter(JPAFilterAndConverter<Object, Object> filterAndConverter) {
      super(filterAndConverter);
   }

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(Cache cache) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cache.getCacheManager());
      isCompatMode = cache.getCacheConfiguration().compatibility().enabled();
   }

   @Override
   public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      ObjectFilter.FilterResult filterResult = filterAndConverter.filterAndConvert(key, newValue, newMetadata);
      if (filterResult != null) {
         Object result = new FilterResult(filterResult.getInstance(), filterResult.getProjection(), filterResult.getSortProjection());
         if (!isCompatMode) {
            try {
               result = ProtobufUtil.toWrappedByteArray(serCtx, result);
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
         }
         return result;
      }
      return null;
   }

   public static final class Externalizer extends AbstractExternalizer<JPAProtobufCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, JPAProtobufCacheEventFilterConverter object) throws IOException {
         output.writeObject(object.filterAndConverter);
      }

      @Override
      public JPAProtobufCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         JPAFilterAndConverter filterAndConverter = (JPAFilterAndConverter) input.readObject();
         return new JPAProtobufCacheEventFilterConverter(filterAndConverter);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.JPA_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends JPAProtobufCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(JPAProtobufCacheEventFilterConverter.class);
      }
   }
}
