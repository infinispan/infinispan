package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.function.RemovableFunction;

/**
 * Implementation of {@link java.util.function.Function} that uses a convert utilizing the unbox value method.
 * @author wburns
 * @since 9.0
 */
public class ConverterValueMapper<V> implements RemovableFunction<V, V>, Serializable {
   private transient TypeConverter converter;

   @Inject
   public void injectFactory(TypeConverter converter) {
      this.converter = converter;
   }

   @Override
   public V apply(V k) {
      return (V) converter.unboxValue(k);
   }

   public static class Externalizer implements AdvancedExternalizer<ConverterValueMapper> {

      @Override
      public Set<Class<? extends ConverterValueMapper>> getTypeClasses() {
         return Collections.singleton(ConverterValueMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.CONVERTER_VALUE_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, ConverterValueMapper object) throws IOException {

      }

      @Override
      public ConverterValueMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ConverterValueMapper();
      }
   }
}
