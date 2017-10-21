package org.infinispan.factories;

import org.infinispan.commons.dataconversion.BinaryEncoder;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.GenericJbossMarshallerEncoder;
import org.infinispan.commons.dataconversion.GlobalMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.dataconversion.JavaCompatEncoder;
import org.infinispan.commons.dataconversion.JavaSerializationEncoder;
import org.infinispan.commons.dataconversion.TextTranscoder;
import org.infinispan.commons.dataconversion.UTF8CompatEncoder;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.core.EncoderRegistryImpl;

/**
 * Factory for {@link EncoderRegistryImpl} objects.
 *
 * @since 9.1
 */
@DefaultFactoryFor(classes = {EncoderRegistry.class})
public class EncoderRegistryFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private StreamingMarshaller globalMarshaller;

   @Override
   public <T> T construct(Class<T> componentType) {
      EncoderRegistryImpl encoderRegistry = new EncoderRegistryImpl();
      encoderRegistry.registerEncoder(IdentityEncoder.INSTANCE);
      encoderRegistry.registerEncoder(UTF8Encoder.INSTANCE);
      encoderRegistry.registerEncoder(JavaSerializationEncoder.INSTANCE);
      encoderRegistry.registerEncoder(new BinaryEncoder(globalMarshaller));
      encoderRegistry.registerEncoder(GenericJbossMarshallerEncoder.INSTANCE);
      encoderRegistry.registerEncoder(new GlobalMarshallerEncoder(globalMarshaller));
      encoderRegistry.registerEncoder(new CompatModeEncoder(globalMarshaller));
      encoderRegistry.registerEncoder(JavaCompatEncoder.INSTANCE);
      encoderRegistry.registerEncoder(UTF8CompatEncoder.INSTANCE);
      encoderRegistry.registerTranscoder(TextTranscoder.INSTANCE);

      encoderRegistry.registerWrapper(ByteArrayWrapper.INSTANCE);
      encoderRegistry.registerWrapper(IdentityWrapper.INSTANCE);
      return componentType.cast(encoderRegistry);
   }

   @Inject
   public void injectDependencies(StreamingMarshaller globalMarshaller) {
      this.globalMarshaller = globalMarshaller;
   }

}
