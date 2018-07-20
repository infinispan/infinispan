package org.infinispan.marshall.persistence.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.atomic.impl.AtomicKeySetImpl;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.MessageMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.util.KeyValuePair;

/**
 * Class responsible for initialising the provided {@link org.infinispan.protostream.SerializationContext} with all of
 * the required {@link org.infinispan.protostream.MessageMarshaller} implementations and .proto files for persistence.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class PersistenceContext implements SerializationContext.MarshallerProvider {

   private static final String PROTO_FILE = "org/infinispan/persistence/persistence.proto";

   public static void init(GlobalComponentRegistry gcr, PersistenceMarshaller pm) throws IOException {
      ClassLoader classLoader = gcr.getGlobalConfiguration().classLoader();
      SerializationContext ctx = pm.getSerializationContext();
      ctx.registerProtoFiles(FileDescriptorSource.fromResources(classLoader, PROTO_FILE));
      ctx.registerMarshaller(new AtomicKeySetImpl.Marshaller(gcr));
      ctx.registerMarshaller(new ByteBufferMarshaller());
      ctx.registerMarshaller(new EntryVersionMarshaller());
      ctx.registerMarshaller(new InternalMetadataImpl.Marshaller());
      ctx.registerMarshaller(new KeyValuePair.Marshaller(pm));
      ctx.registerMarshaller(new MarshalledEntryImpl.Marshaller(pm));
      ctx.registerMarshaller(new MapMarshaller.TypeMarshaller());
      ctx.registerMarshaller(new MetadataMarshaller.TypeMarshaller());
      ctx.registerMarshaller(new NumericVersion.Marshaller());
      ctx.registerMarshaller(new SimpleClusteredVersion.Marshaller());
      ctx.registerMarshaller(new UserObject.Marshaller(pm.getUserMarshaller()));
      ctx.registerMarshaller(new WrappedByteArrayMarshaller());
      ctx.registerMarshallerProvider(new PersistenceContext());
   }

   private final Map<String, BaseMarshaller> marshallerMap = new HashMap<>();

   private PersistenceContext() {
      addToLookupMap(new MetadataMarshaller(), "persistence.Metadata", Metadata.class);
      addToLookupMap(new MapMarshaller(), "persistence.Map", Map.class);
   }

   @Override
   public BaseMarshaller<?> getMarshaller(String s) {
      return marshallerMap.get(s);
   }

   @Override
   public BaseMarshaller<?> getMarshaller(Class<?> aClass) {
      if (Metadata.class.isAssignableFrom(aClass))
         return marshallerMap.get(Metadata.class.getName());
      else if (Map.class.isAssignableFrom(aClass))
         return marshallerMap.get(Map.class.getName());
      return null;
   }

   private void addToLookupMap(MessageMarshaller<?>  marshaller, String messageName, Class lookupClass) {
      marshallerMap.put(messageName, marshaller);
      marshallerMap.put(lookupClass.getName(), marshaller);
   }
}
