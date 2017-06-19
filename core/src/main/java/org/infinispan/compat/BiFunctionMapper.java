package org.infinispan.compat;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;
import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

public class BiFunctionMapper implements BiFunction {

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;
   private final BiFunction biFunction;

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(keyEncoderClass);
      this.valueEncoder = encoderRegistry.getEncoder(valueEncoderClass);
      this.keyWrapper = encoderRegistry.getWrapper(keyWrapperClass);
      this.valueWrapper = encoderRegistry.getWrapper(valueWrapperClass);
   }

   public BiFunctionMapper(BiFunction remappingFunction,
                           Class<? extends Encoder> keyEncoderClass,
                           Class<? extends Encoder> valueEncoderClass,
                           Class<? extends Wrapper> keyWrapperClass,
                           Class<? extends Wrapper> valueWrapperClass) {
      this.biFunction = remappingFunction;
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   @Override
   public Object apply(Object k, Object v) {
      Object key = fromStorage(k, keyEncoder, keyWrapper);
      Object value = fromStorage(v, valueEncoder, valueWrapper);
      Object result = biFunction.apply(key, value);
      return result != null ? toStorage(result, valueEncoder, valueWrapper) : null;
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
         output.writeObject(object.keyEncoderClass);
         output.writeObject(object.valueEncoderClass);
         output.writeObject(object.keyWrapperClass);
         output.writeObject(object.valueWrapperClass);
      }

      @Override
      public BiFunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiFunctionMapper((BiFunction) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject());
      }
   }
}
