package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;

public class FunctionMapper<K, V, KT, VT> implements Function<KT, VT> {

   private transient TypeConverter<K, V, KT, VT> converter;
   private final Function<? super K, ? extends V> function;

   @Inject
   public void injectDependencies(TypeConverter converter) {
      this.converter = converter;
   }

   public FunctionMapper(Function<? super K, ? extends V> mappingFunction) {
      this.function = mappingFunction;
   }

   public FunctionMapper(Function<? super K, ? extends V> mappingFunction,
                         TypeConverter converter) {
      this.function = mappingFunction;
      this.converter = converter;
   }

   public Function<? super K, ? extends V> getFunction() {
      return function;
   }

   @Override
   public VT apply(KT k) {
      return converter.boxValue(function.apply(converter.unboxKey(k)));
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
      }

      @Override
      public FunctionMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new FunctionMapper((Function) input.readObject());
      }
   }
}
