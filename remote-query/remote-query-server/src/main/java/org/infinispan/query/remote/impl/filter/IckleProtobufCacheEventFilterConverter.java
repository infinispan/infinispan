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
import org.infinispan.query.dsl.embedded.impl.IckleCacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.IckleFilterAndConverter;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.RemoteQueryManager;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public final class IckleProtobufCacheEventFilterConverter extends IckleCacheEventFilterConverter<Object, Object, Object> {

   private RemoteQueryManager remoteQueryManager;

   public IckleProtobufCacheEventFilterConverter(IckleFilterAndConverter<Object, Object> filterAndConverter) {
      super(filterAndConverter);
   }

   @Inject
   @SuppressWarnings("unused")
   protected void injectDependencies(Cache cache) {
      remoteQueryManager = cache.getAdvancedCache().getComponentRegistry().getComponent(RemoteQueryManager.class);
   }

   @Override
   public Object filterAndConvert(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      ObjectFilter.FilterResult filterResult = filterAndConverter.filterAndConvert(key, newValue, newMetadata);
      if (filterResult != null) {
         Object result = new FilterResult(filterResult.getInstance(), filterResult.getProjection(), filterResult.getSortProjection());
         return remoteQueryManager.encodeFilterResult(result);
      }
      return null;
   }

   public static final class Externalizer extends AbstractExternalizer<IckleProtobufCacheEventFilterConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleProtobufCacheEventFilterConverter object) throws IOException {
         output.writeObject(object.filterAndConverter);
      }

      @Override
      public IckleProtobufCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         IckleFilterAndConverter filterAndConverter = (IckleFilterAndConverter) input.readObject();
         return new IckleProtobufCacheEventFilterConverter(filterAndConverter);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleProtobufCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(IckleProtobufCacheEventFilterConverter.class);
      }
   }
}
