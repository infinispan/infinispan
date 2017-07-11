package org.infinispan.compat;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;
import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

public class EntryConsumerMapper implements Consumer<Map.Entry> {

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;

   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;
   private final BiConsumer action;


   public EntryConsumerMapper(BiConsumer action,
                              Class<? extends Encoder> keyEncoderClass,
                              Class<? extends Encoder> valueEncoderClass,
                              Class<? extends Wrapper> keyWrapperClass,
                              Class<? extends Wrapper> valueWrapperClass) {
      this.action = action;
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(keyEncoderClass);
      this.valueEncoder = encoderRegistry.getEncoder(valueEncoderClass);
      this.keyWrapper = encoderRegistry.getWrapper(keyWrapperClass);
      this.valueWrapper = encoderRegistry.getWrapper(valueWrapperClass);
   }

   @Override
   public void accept(Map.Entry entry) {
      Object key = fromStorage(entry.getKey(), keyEncoder, keyWrapper);
      Object value = toStorage(entry.getValue(), valueEncoder, valueWrapper);
      action.accept(key, value);

   }

   public static class Externalizer implements AdvancedExternalizer<EntryConsumerMapper> {
      @Override
      public Set<Class<? extends EntryConsumerMapper>> getTypeClasses() {
         return Collections.singleton(EntryConsumerMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENTRY_CONSUMER_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, EntryConsumerMapper object) throws IOException {
         output.writeObject(object.action);
         output.writeObject(object.keyEncoderClass);
         output.writeObject(object.valueEncoderClass);
         output.writeObject(object.keyWrapperClass);
         output.writeObject(object.valueWrapperClass);
      }

      @Override
      public EntryConsumerMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EntryConsumerMapper((BiConsumer) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Encoder>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject());
      }
   }
}
