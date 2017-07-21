package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.function.RemovableFunction;

/**
 * {@link java.util.function.Function} that uses a keyEncoder to converter keys from the configured storage format to
 * the requested format.
 *
 * @since 9.1
 */
public class EncoderKeyMapper<K> implements RemovableFunction<K, K> {
   private final DataConversion dataConversion;

   public EncoderKeyMapper(DataConversion dataConversion) {
      this.dataConversion = dataConversion;
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(dataConversion);
   }

   @Override
   @SuppressWarnings("unchecked")
   public K apply(K k) {
      return (K) dataConversion.fromStorage(k);
   }

   public static class Externalizer implements AdvancedExternalizer<EncoderKeyMapper> {

      @Override
      public Set<Class<? extends EncoderKeyMapper>> getTypeClasses() {
         return Collections.singleton(EncoderKeyMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODER_KEY_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, EncoderKeyMapper object) throws IOException {
         output.writeObject(object.dataConversion);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderKeyMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderKeyMapper((DataConversion) input.readObject());
      }
   }
}
