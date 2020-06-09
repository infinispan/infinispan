package org.infinispan.factories;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.BinaryTranscoder;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.GlobalMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.dataconversion.JavaSerializationEncoder;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.encoding.ProtostreamTranscoder;
import org.infinispan.encoding.impl.JavaSerializationTranscoder;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.core.EncoderRegistryImpl;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.PersistenceContextInitializer;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;

/**
 * Factory for {@link EncoderRegistryImpl} objects.
 *
 * @since 9.1
 */
@DefaultFactoryFor(classes = {EncoderRegistry.class})
public class EncoderRegistryFactory extends AbstractComponentFactory implements AutoInstantiableFactory {
   // Must not start the global marshaller or it will be too late for modules to register their externalizers
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   ComponentRef<StreamingMarshaller> globalMarshaller;
   @Inject @ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER)
   PersistenceMarshaller persistenceMarshaller;
   @Inject EmbeddedCacheManager embeddedCacheManager;
   @Inject SerializationContextRegistry ctxRegistry;

   @Override
   public Object construct(String componentName) {
      ClassLoader classLoader = globalConfiguration.classLoader();
      EncoderRegistryImpl encoderRegistry = new EncoderRegistryImpl();
      ClassWhiteList classWhiteList = embeddedCacheManager.getClassWhiteList();
      // TODO Move registration to GlobalMarshaller ISPN-9622
      String messageName = PersistenceContextInitializer.getFqTypeName(MarshallableUserObject.class);
      Marshaller userMarshaller = persistenceMarshaller.getUserMarshaller();
      ctxRegistry.addMarshaller(SerializationContextRegistry.MarshallerType.GLOBAL,
                                new MarshallableUserObject.Marshaller(messageName, userMarshaller));

      encoderRegistry.registerEncoder(IdentityEncoder.INSTANCE);
      encoderRegistry.registerEncoder(UTF8Encoder.INSTANCE);

      encoderRegistry.registerEncoder(new JavaSerializationEncoder(classWhiteList));
      encoderRegistry.registerEncoder(new GlobalMarshallerEncoder(globalMarshaller.wired()));

      // Default and binary transcoder use the user marshaller to convert data to/from a byte array
      encoderRegistry.registerTranscoder(new DefaultTranscoder(userMarshaller));
      encoderRegistry.registerTranscoder(new BinaryTranscoder(userMarshaller));
      // Core transcoders are always available
      encoderRegistry.registerTranscoder(new ProtostreamTranscoder(ctxRegistry, classLoader));
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder(classWhiteList));
      // Wraps the GlobalMarshaller so that it can be used as a transcoder
      // Keeps application/x-infinispan-marshalling available for backwards compatibility
      encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(globalMarshaller.wired()));
      // Make the user marshaller's media type available as well
      // As custom marshaller modules like Kryo and Protostuff do not define their own transcoder
      encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(userMarshaller));

      encoderRegistry.registerWrapper(ByteArrayWrapper.INSTANCE);
      encoderRegistry.registerWrapper(IdentityWrapper.INSTANCE);
      return encoderRegistry;
   }
}
