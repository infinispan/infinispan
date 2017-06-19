package org.infinispan.compat;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;
import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

public class FunctionMapper implements Function {

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;

   private final Function function;

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(keyEncoderClass);
      this.valueEncoder = encoderRegistry.getEncoder(valueEncoderClass);
      this.keyWrapper = encoderRegistry.getWrapper(keyWrapperClass);
      this.valueWrapper = encoderRegistry.getWrapper(valueWrapperClass);
   }

   public FunctionMapper(Function mappingFunction,
                         Class<? extends Encoder> keyEncoderClass,
                         Class<? extends Encoder> valueEncoderClass,
                         Class<? extends Wrapper> keyWrapperClass,
                         Class<? extends Wrapper> valueWrapperClass) {
      this.function = mappingFunction;
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   @Override
   public Object apply(Object k) {
      Object key = fromStorage(k, keyEncoder, keyWrapper);
      Object result = function.apply(key);
      return result != null ? toStorage(result, valueEncoder, valueWrapper) : null;
   }

   public static class Externalizer implements AdvancedExternalizer<FunctionMapper> {

      @Override
      public Set<Class<? extends FunctionMapper>> getTypeClasses() {
         return Collections.singleton(FunctionMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.FUNCTION_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, FunctionMapper object) throws IOException {
         output.writeObject(object.function);
         output.writeObject(object.keyEncoderClass);
         output.writeObject(object.valueEncoderClass);
         output.writeObject(object.keyWrapperClass);
         output.writeObject(object.valueWrapperClass);
      }

      @Override
      public FunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new FunctionMapper((Function) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject());
      }
   }
}
