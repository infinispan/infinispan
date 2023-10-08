package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Adapter for {@link IckleProtobufFilterAndConverter} that produces binary values as a result of filter/conversion.
 *
 * @author gustavonalle
 * @since 8.1
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_ICKLE_BINARY_PROTOBUF_FILTER_AND_CONVERTER)
@Scope(Scopes.NONE)
public final class IckleBinaryProtobufFilterAndConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, Object> {

   private SerializationContext serCtx;

   @ProtoField(1)
   final IckleProtobufFilterAndConverter delegate;

   @Inject
   void injectDependencies(ComponentRegistry componentRegistry, EmbeddedCacheManager cacheManager) {
      componentRegistry.wireDependencies(delegate);
      serCtx = SecurityActions.getSerializationContext(cacheManager);
   }

   IckleBinaryProtobufFilterAndConverter(String queryString, Map<String, Object> namedParameters) {
      this.delegate = new IckleProtobufFilterAndConverter(queryString, namedParameters);
   }

   @ProtoFactory
   IckleBinaryProtobufFilterAndConverter(IckleProtobufFilterAndConverter delegate) {
      this.delegate = delegate;
   }

   @Override
   public Object filterAndConvert(K key, V value, Metadata metadata) {
      Optional<ObjectFilter.FilterResult> filterResult = Optional.ofNullable(delegate.filterAndConvert(key, value, metadata));
      return filterResult.map(fr -> {
         Object instance = fr.getInstance();
         if (instance != null)
            return instance;
         return Arrays.stream(fr.getProjection()).map(this::toByteArray).toArray();
      }).orElse(null);
   }

   private Object toByteArray(Object ref) {
      try {
         return ProtobufUtil.toWrappedByteArray(serCtx, ref);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public MediaType format() {
      return MediaType.APPLICATION_PROTOSTREAM;
   }
}
