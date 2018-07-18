package org.infinispan.cache.impl;

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

/**
 * {@link java.util.function.Function} that uses a valueEncoder to converter values from the configured storage format
 * to the requested format.
 *
 * @since 9.1
 */
public class EncoderValueMapper<V> implements Function<V, V> {

   private final DataConversion dataConversion;


   public EncoderValueMapper(DataConversion dataConversion) {
      this.dataConversion = dataConversion;
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(dataConversion);
   }

   @Override
   @SuppressWarnings("unchecked")
   public V apply(V v) {
      return (V) dataConversion.fromStorage(v);
   }

   public static class Externalizer implements AdvancedExternalizer<EncoderValueMapper> {

      @Override
      public Set<Class<? extends EncoderValueMapper>> getTypeClasses() {
         return Collections.singleton(EncoderValueMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODER_VALUE_MAPPER;
      }

      @Override
      public void writeObject(UserObjectOutput output, EncoderValueMapper object) throws IOException {
         DataConversion.writeTo(output, object.dataConversion);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderValueMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderValueMapper(DataConversion.readFrom(input));
      }
   }
}
