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
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.encoding.ProtostreamTranscoder;
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
      ctxRegistry.addMarshaller(SerializationContextRegistry.MarshallerType.GLOBAL, new MarshallableUserObject.Marshaller(messageName, persistenceMarshaller.getUserMarshaller()));

      encoderRegistry.registerEncoder(IdentityEncoder.INSTANCE);
      encoderRegistry.registerEncoder(UTF8Encoder.INSTANCE);
      encoderRegistry.registerEncoder(new JavaSerializationEncoder(classWhiteList));
      encoderRegistry.registerEncoder(new GlobalMarshallerEncoder(globalMarshaller.wired()));
      encoderRegistry.registerTranscoder(new DefaultTranscoder(persistenceMarshaller.getUserMarshaller()));
      encoderRegistry.registerTranscoder(new BinaryTranscoder(persistenceMarshaller.getUserMarshaller()));
      // Wraps the GlobalMarshaller so that it can be used as a transcoder
      encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(globalMarshaller.wired()));
      encoderRegistry.registerTranscoder(new ProtostreamTranscoder(ctxRegistry, classLoader));

      encoderRegistry.registerWrapper(ByteArrayWrapper.INSTANCE);
      encoderRegistry.registerWrapper(IdentityWrapper.INSTANCE);
      return encoderRegistry;
   }
}
