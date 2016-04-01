package org.infinispan.query.remote.impl.filter;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class JPAProtobufCacheEventFilterConverter extends JPACacheEventFilterConverter<Object, Object, byte[]> {

   private SerializationContext serCtx;

   public JPAProtobufCacheEventFilterConverter(JPAFilterAndConverter<Object, Object> filterAndConverter) {
      super(filterAndConverter);
   }

   @Inject
   protected void injectDependencies(EmbeddedCacheManager cacheManager) {
      serCtx = ProtobufMetadataManagerImpl.getSerializationContextInternal(cacheManager);
   }

   @Override
   public byte[] filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      ObjectFilter.FilterResult filterResult = filterAndConverter.filterAndConvert(key, newValue, newMetadata);
      if (filterResult != null) {
         try {
            return ProtobufUtil.toWrappedByteArray(serCtx, new FilterResult(filterResult.getInstance(), filterResult.getProjection(), filterResult.getSortProjection()));
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
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
