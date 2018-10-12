package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.impl.ExternalizerIds;
import org.infinispan.query.remote.impl.ProtobufMetadataManagerImpl;

/**
 * Adapter for {@link IckleProtobufFilterAndConverter} that produces binary values as a result of filter/conversion.
 *
 * @author gustavonalle
 * @since 8.1
 */
public final class IckleBinaryProtobufFilterAndConverter<K, V> extends AbstractKeyValueFilterConverter<K, V, Object> {

   private SerializationContext serCtx;

   private final IckleProtobufFilterAndConverter delegate;

   @Inject
   void injectDependencies(ComponentRegistry componentRegistry, EmbeddedCacheManager cacheManager) {
      componentRegistry.wireDependencies(delegate);
      serCtx = ProtobufMetadataManagerImpl.getSerializationContext(cacheManager);
   }

   IckleBinaryProtobufFilterAndConverter(String queryString, Map<String, Object> namedParameters) {
      this.delegate = new IckleProtobufFilterAndConverter(queryString, namedParameters);
   }

   private IckleBinaryProtobufFilterAndConverter(IckleProtobufFilterAndConverter delegate) {
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

   public static final class Externalizer extends AbstractExternalizer<IckleBinaryProtobufFilterAndConverter> {

      @Override
      public void writeObject(ObjectOutput output, IckleBinaryProtobufFilterAndConverter object) throws IOException {
         output.writeObject(object.delegate);
      }

      @Override
      public IckleBinaryProtobufFilterAndConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         IckleProtobufFilterAndConverter delegate = (IckleProtobufFilterAndConverter) input.readObject();
         return new IckleBinaryProtobufFilterAndConverter(delegate);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_BINARY_PROTOBUF_FILTER_AND_CONVERTER;
      }

      @Override
      public Set<Class<? extends IckleBinaryProtobufFilterAndConverter>> getTypeClasses() {
         return Collections.singleton(IckleBinaryProtobufFilterAndConverter.class);
      }
   }

}
