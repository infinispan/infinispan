package org.infinispan.compat;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;
import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

public class FunctionMapper implements Function, InjectableComponent {

   private final EncodingClasses encodingClasses;

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;

   private final Function function;

   public FunctionMapper(Function mappingFunction,
                         EncodingClasses encodingClasses) {
      this.function = mappingFunction;
      this.encodingClasses = encodingClasses;
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(encodingClasses.getKeyEncoderClass());
      this.valueEncoder = encoderRegistry.getEncoder(encodingClasses.getValueEncoderClass());
      this.keyWrapper = encoderRegistry.getWrapper(encodingClasses.getKeyWrapperClass());
      this.valueWrapper = encoderRegistry.getWrapper(encodingClasses.getValueWrapperClass());
   }

   @Override
   public Object apply(Object k) {
      Object key = fromStorage(k, keyEncoder, keyWrapper);
      Object result = function.apply(key);
      return result == null ? result : toStorage(result, valueEncoder, valueWrapper);
   }

   @Override
   public void inject(ComponentRegistry registry) {
      registry.wireDependencies(this);
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
         EncodingClasses.writeTo(output, object.encodingClasses);
      }

      @Override
      public FunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new FunctionMapper((Function) input.readObject(),
               EncodingClasses.readFrom(input));
      }
   }
}
