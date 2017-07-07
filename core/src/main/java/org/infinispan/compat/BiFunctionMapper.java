package org.infinispan.compat;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;
import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

public class BiFunctionMapper implements BiFunction {

   private EncodingClasses encodingClasses;

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;
   private final BiFunction biFunction;

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(encodingClasses.getKeyEncoderClass());
      this.valueEncoder = encoderRegistry.getEncoder(encodingClasses.getValueEncoderClass());
      this.keyWrapper = encoderRegistry.getWrapper(encodingClasses.getKeyWrapperClass());
      this.valueWrapper = encoderRegistry.getWrapper(encodingClasses.getValueWrapperClass());
   }

   public BiFunctionMapper(BiFunction remappingFunction,
                           EncodingClasses encodingClasses) {
      this.biFunction = remappingFunction;
      this.encodingClasses = encodingClasses;
   }

   @Override
   public Object apply(Object k, Object v) {
      Object key = fromStorage(k, keyEncoder, keyWrapper);
      Object value = fromStorage(v, valueEncoder, valueWrapper);
      Object result = biFunction.apply(key, value);
      return result == null ? result : toStorage(result, valueEncoder, valueWrapper);
   }

   public static class Externalizer implements AdvancedExternalizer<BiFunctionMapper> {

      @Override
      public Set<Class<? extends BiFunctionMapper>> getTypeClasses() {
         return Collections.singleton(BiFunctionMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.BI_FUNCTION_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, BiFunctionMapper object) throws IOException {
         output.writeObject(object.biFunction);
         EncodingClasses.writeTo(output, object.encodingClasses);
      }

      @Override
      public BiFunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiFunctionMapper((BiFunction) input.readObject(),
               EncodingClasses.readFrom(input));
      }
   }
}
