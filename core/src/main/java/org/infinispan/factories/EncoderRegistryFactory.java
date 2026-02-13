package org.infinispan.factories;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.BinaryTranscoder;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.DefaultTranscoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.dataconversion.TextTranscoder;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.encoding.ProtostreamTranscoder;
import org.infinispan.encoding.impl.JavaSerializationTranscoder;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.core.EncoderRegistryImpl;
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
   ComponentRef<Marshaller> globalMarshaller;
   @Inject @ComponentName(KnownComponentNames.USER_MARSHALLER)
   Marshaller userMarshaller;

   @Inject EmbeddedCacheManager embeddedCacheManager;
   @Inject SerializationContextRegistry ctxRegistry;

   @Override
   public Object construct(String componentName) {
      ClassLoader classLoader = globalConfiguration.classLoader();
      EncoderRegistryImpl encoderRegistry = new EncoderRegistryImpl();
      ClassAllowList classAllowList = embeddedCacheManager.getClassAllowList();

      // Deprecated binary transcoder. Will be removed in a future version together with MediaType.APPLICATION_UNKNOWN.
      encoderRegistry.registerTranscoder(new BinaryTranscoder(userMarshaller));
      // Core transcoders are always available
      encoderRegistry.registerTranscoder(new ProtostreamTranscoder(ctxRegistry, classLoader));
      encoderRegistry.registerTranscoder(new JavaSerializationTranscoder(classAllowList));
      encoderRegistry.registerTranscoder(TextTranscoder.INSTANCE);
      // Default transcoder use the global marshaller to convert data to/from a byte array
      encoderRegistry.registerTranscoder(new DefaultTranscoder(globalMarshaller.wired()));
      // Make the user marshaller's media type available as well
      // As custom marshaller modules like Kryo and Protostuff do not define their own transcoder
      encoderRegistry.registerTranscoder(new TranscoderMarshallerAdapter(userMarshaller));

      encoderRegistry.registerWrapper(ByteArrayWrapper.INSTANCE);
      encoderRegistry.registerWrapper(IdentityWrapper.INSTANCE);
      return encoderRegistry;
   }
}
