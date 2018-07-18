package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;

public class FunctionMapper implements Function {

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   private final Function function;

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(keyDataConversion);
      registry.wireDependencies(valueDataConversion);
   }

   public FunctionMapper(Function mappingFunction,
                         DataConversion keyDataConversion,
                         DataConversion valueDataConversion) {
      this.function = mappingFunction;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Override
   public Object apply(Object k) {
      Object key = keyDataConversion.fromStorage(k);
      Object result = function.apply(key);
      return result != null ? valueDataConversion.toStorage(result) : null;
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
      public void writeObject(UserObjectOutput output, FunctionMapper object) throws IOException {
         output.writeObject(object.function);
         DataConversion.writeTo(output, object.keyDataConversion);
         DataConversion.writeTo(output, object.valueDataConversion);
      }

      @Override
      public FunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new FunctionMapper((Function) input.readObject(),
               DataConversion.readFrom(input), DataConversion.readFrom(input));
      }
   }
}
