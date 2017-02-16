package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.function.RemovableFunction;

/**
 * Implementation of {@link java.util.function.Function} that uses a convert utilizing the unbox key method.
 * @author wburns
 * @since 9.0
 */
public class ConverterKeyMapper<K> implements RemovableFunction<K, K> {
   private transient TypeConverter converter;

   @Inject
   public void injectFactory(TypeConverter converter) {
      this.converter = converter;
   }

   @Override
   public K apply(K k) {
      return (K) converter.unboxKey(k);
   }

   public static class Externalizer implements AdvancedExternalizer<ConverterKeyMapper> {

      @Override
      public Set<Class<? extends ConverterKeyMapper>> getTypeClasses() {
         return Collections.singleton(ConverterKeyMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.CONVERTER_KEY_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, ConverterKeyMapper object) throws IOException {

      }

      @Override
      public ConverterKeyMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ConverterKeyMapper();
      }
   }
}
