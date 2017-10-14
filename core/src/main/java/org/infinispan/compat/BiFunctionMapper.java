package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;

public class BiFunctionMapper implements BiFunction {

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   private final BiFunction biFunction;

   @Inject
   public void injectDependencies(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
   }

   public BiFunctionMapper(BiFunction remappingFunction,
                           DataConversion keyDataConversion,
                           DataConversion valueDataConversion) {
      this.biFunction = remappingFunction;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Override
   public Object apply(Object k, Object v) {
      Object key = keyDataConversion.fromStorage(k);
      Object value = valueDataConversion.fromStorage(v);
      Object result = biFunction.apply(key, value);
      return result != null ? valueDataConversion.toStorage(result) : null;
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
         DataConversion.writeTo(output, object.keyDataConversion);
         DataConversion.writeTo(output, object.valueDataConversion);
      }

      @Override
      public BiFunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiFunctionMapper((BiFunction) input.readObject(),
               DataConversion.readFrom(input), DataConversion.readFrom(input));
      }
   }
}
