package org.infinispan.encoding;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * Handle conversions for Keys or values.
 *
 * @since 9.2
 */
public class DataConversion {

   public static final DataConversion DEFAULT = new DataConversion(IdentityEncoder.INSTANCE, ByteArrayWrapper.INSTANCE);

   private final Class<? extends Encoder> encoderClass;
   private final Class<? extends Wrapper> wrapperClass;

   private Encoder encoder;
   private Wrapper wrapper;

   public DataConversion(Class<? extends Encoder> encoderClass, Class<? extends Wrapper> wrapperClass) {
      this.encoderClass = encoderClass;
      this.wrapperClass = wrapperClass;
   }

   private DataConversion(Encoder encoder, Wrapper wrapper) {
      this.encoder = encoder;
      this.wrapper = wrapper;
      this.encoderClass = encoder.getClass();
      this.wrapperClass = wrapper.getClass();
   }

   public DataConversion withEncoding(Class<? extends Encoder> encoderClass) {
      return new DataConversion(encoderClass, this.wrapperClass);
   }

   public DataConversion withWrapping(Class<? extends Wrapper> wrapperClass) {
      return new DataConversion(this.encoderClass, wrapperClass);
   }


   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry, Configuration configuration) {
      if (!CompatModeEncoder.class.equals(encoderClass)) {
         this.encoder = encoderRegistry.getEncoder(encoderClass);
      } else {
         CompatibilityModeConfiguration compatibility = configuration.compatibility();
         Marshaller compatMarshaller = compatibility.marshaller();
         this.encoder = new CompatModeEncoder(compatMarshaller);
      }
      this.wrapper = encoderRegistry.getWrapper(wrapperClass);
   }

   public Object fromStorage(Object stored) {
      if (encoder == null || wrapper == null) {
         throw new IllegalArgumentException("Both Encoder and Wrapper must be provided!");
      }
      if (stored == null) return null;
      return encoder.fromStorage(wrapper.unwrap(stored));

   }

   public Object toStorage(Object toStore) {
      if (encoder == null || wrapper == null) {
         throw new IllegalArgumentException("Both Encoder and Wrapper must be provided!");
      }
      if (toStore == null) return null;
      return wrapper.wrap(encoder.toStorage(toStore));
   }

   public Encoder getEncoder() {
      return encoder;
   }

   public Wrapper getWrapper() {
      return wrapper;
   }

   public Class<? extends Encoder> getEncoderClass() {
      return encoderClass;
   }

   public Class<? extends Wrapper> getWrapperClass() {
      return wrapperClass;
   }

   public boolean isStorageFormatFilterable() {
      return encoder.isStorageFormatFilterable();
   }

   public static class Externalizer extends AbstractExternalizer<DataConversion> {

      @Override
      public Set<Class<? extends DataConversion>> getTypeClasses() {
         return Util.asSet(DataConversion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataConversion dataConversion) throws IOException {
         output.writeObject(dataConversion.encoderClass);
         output.writeObject(dataConversion.wrapperClass);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_CONVERSION;
      }

      @Override
      public DataConversion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Class<? extends Encoder> encoderClass = (Class<? extends Encoder>) input.readObject();
         Class<? extends Wrapper> wrapperClass = (Class<? extends Wrapper>) input.readObject();
         return new DataConversion(encoderClass, wrapperClass);
      }
   }
}
