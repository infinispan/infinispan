package org.infinispan.factories;

import org.infinispan.commons.dataconversion.BinaryEncoder;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.GenericJbossMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.JavaSerializationEncoder;
import org.infinispan.commons.dataconversion.MarshallerEncoder;
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
      encoderRegistry.registerEncoder(new JavaSerializationEncoder());
      encoderRegistry.registerEncoder(new BinaryEncoder(globalMarshaller));
      encoderRegistry.registerEncoder(new MarshallerEncoder(globalMarshaller));
      encoderRegistry.registerEncoder(new GenericJbossMarshallerEncoder());

      encoderRegistry.registerWrapper(new ByteArrayWrapper());
      return componentType.cast(encoderRegistry);
   }

   @Inject
   public void injectDependencies(StreamingMarshaller globalMarshaller) {
      this.globalMarshaller = globalMarshaller;
   }

}
