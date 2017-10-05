package org.infinispan.encoding;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
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

   private Class<? extends Encoder> encoderClass;
   private Class<? extends Wrapper> wrapperClass;
   private Short encoderId;
   private Byte wrapperId;
   private Encoder encoder;
   private Wrapper wrapper;

   public DataConversion(Class<? extends Encoder> encoderClass, Class<? extends Wrapper> wrapperClass) {
      this.encoderClass = encoderClass;
      this.wrapperClass = wrapperClass;
   }

   private DataConversion(Short encoderId, Byte wrapperId) {
      this.encoderId = encoderId;
      this.wrapperId = wrapperId;
   }

   private DataConversion(Encoder encoder, Wrapper wrapper) {
      this.encoder = encoder;
      this.wrapper = wrapper;
      this.encoderClass = encoder.getClass();
      this.wrapperClass = wrapper.getClass();
   }

   public DataConversion withEncoding(Class<? extends Encoder> encoderClass) {
      if (Objects.equals(this.encoderClass, encoderClass)) return this;
      return new DataConversion(encoderClass, this.wrapperClass);
   }

   public DataConversion withWrapping(Class<? extends Wrapper> wrapperClass) {
      if (Objects.equals(this.wrapperClass, wrapperClass)) return this;
      return new DataConversion(this.encoderClass, wrapperClass);
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry, Configuration configuration) {
      if (wrapper == null) {
         this.wrapper = encoderRegistry.getWrapper(wrapperClass, wrapperId);
      }
      if (!CompatModeEncoder.class.equals(encoderClass)) {
         if (encoder == null) {
            this.encoder = encoderRegistry.getEncoder(encoderClass, encoderId);
         }
      } else {
         CompatibilityModeConfiguration compatibility = configuration.compatibility();
         Marshaller compatMarshaller = compatibility.marshaller();
         this.encoder = new CompatModeEncoder(compatMarshaller);
      }
   }

   public Object fromStorage(Object stored) {
      if (stored == null) return null;
      checkConverters();
      return encoder.fromStorage(wrapper.unwrap(stored));
   }

   public Object toStorage(Object toStore) {
      if (toStore == null) return null;
      checkConverters();
      return wrapper.wrap(encoder.toStorage(toStore));
   }

   public Object extractIndexable(Object stored) {
      if (stored == null) return null;
      checkConverters();
      if (encoder.isStorageFormatFilterable()) {
         return wrapper.isFilterable() ? stored : wrapper.unwrap(stored);
      }
      return encoder.fromStorage(wrapper.isFilterable() ? stored : wrapper.unwrap(stored));
   }

   private void checkConverters() {
      if (encoder == null || wrapper == null) {
         throw new IllegalArgumentException("Cannot convert object, both Encoder and Wrapper must be non-null!");
      }
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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataConversion that = (DataConversion) o;
      return Objects.equals(encoderClass, that.encoderClass) &&
            Objects.equals(wrapperClass, that.wrapperClass);
   }

   @Override
   public int hashCode() {
      return Objects.hash(encoderClass, wrapperClass);
   }

   public static class Externalizer extends AbstractExternalizer<DataConversion> {

      @Override
      public Set<Class<? extends DataConversion>> getTypeClasses() {
         return Util.asSet(DataConversion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataConversion dataConversion) throws IOException {
         boolean isDefault = dataConversion.equals(DEFAULT);
         output.writeBoolean(isDefault);
         if (!isDefault) {
            output.writeShort(dataConversion.encoder.id());
            output.writeByte(dataConversion.wrapper.id());
         }
      }

      @Override
      public Integer getId() {
         return Ids.DATA_CONVERSION;
      }

      @Override
      public DataConversion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         if (input.readBoolean()) return DataConversion.DEFAULT;
         return new DataConversion(input.readShort(), input.readByte());
      }
   }
}
