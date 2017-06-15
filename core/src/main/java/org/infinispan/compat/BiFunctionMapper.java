package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.BiFunction;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;

public class BiFunctionMapper<K, V, KT, VT> implements BiFunction<KT, VT, VT> {

   private transient TypeConverter<K, V, KT, VT> converter;
   private final BiFunction<? super K, ? super V, ? extends V> biFunction;

   @Inject
   public void injectDependencies(TypeConverter converter) {
      this.converter = converter;
   }

   public BiFunctionMapper(BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      this.biFunction = remappingFunction;
   }

   public BiFunctionMapper(BiFunction<? super K, ? super V, ? extends V> remappingFunction,
                           TypeConverter converter) {
      this.biFunction = remappingFunction;
      this.converter = converter;
   }

   @Override
   public VT apply(KT k, VT v) {
      return converter.boxValue(biFunction.apply(converter.unboxKey(k), converter.unboxValue(v)));
   }

   public BiFunction<? super K, ? super V, ? extends V> getBiFunction() {
      return biFunction;
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
      }

      @Override
      public BiFunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new BiFunctionMapper((BiFunction) input.readObject());
      }
   }
}
