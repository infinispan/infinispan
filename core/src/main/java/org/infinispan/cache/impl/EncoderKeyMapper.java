package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.util.function.RemovableFunction;

/**
 * {@link java.util.function.Function} that uses a keyEncoder to converter keys from the configured storage format to
 * the requested format.
 *
 * @since 9.1
 */
public class EncoderKeyMapper<K> implements RemovableFunction<K, K> {
   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private transient Encoder keyEncoder;
   private transient Wrapper keyWrapper;

   public EncoderKeyMapper(Class<? extends Encoder> encoder, Class<? extends Wrapper> keyWrapperClass) {
      this.keyEncoderClass = encoder;
      this.keyWrapperClass = keyWrapperClass;
   }

   @Inject
   public void injectDependencies(EncoderRegistry registry) {
      this.keyEncoder = registry.getEncoder(keyEncoderClass);
      this.keyWrapper = registry.getWrapper(keyWrapperClass);
   }

   @Override
   @SuppressWarnings("unchecked")
   public K apply(K k) {
      return (K) keyEncoder.fromStorage(keyWrapper.unwrap(k));
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
         output.writeObject(object.keyEncoderClass);
         output.writeObject(object.keyWrapperClass);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderKeyMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderKeyMapper((Class<? extends Encoder>) input.readObject(), (Class<? extends Wrapper>) input.readObject());
      }
   }
}
