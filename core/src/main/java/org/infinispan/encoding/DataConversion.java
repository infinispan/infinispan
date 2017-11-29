package org.infinispan.encoding;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.dataconversion.BinaryEncoder;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.GlobalMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CompatibilityModeConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * Handle conversions for Keys or values.
 *
 * @since 9.2
 */
public class DataConversion {

   public static final DataConversion DEFAULT_KEY = new DataConversion(IdentityEncoder.INSTANCE, ByteArrayWrapper.INSTANCE, true);
   public static final DataConversion DEFAULT_VALUE = new DataConversion(IdentityEncoder.INSTANCE, ByteArrayWrapper.INSTANCE, false);

   private Class<? extends Encoder> encoderClass;
   private Class<? extends Wrapper> wrapperClass;
   private MediaType requestMediaType;
   private MediaType storageMediaType;

   private Short encoderId;
   private Byte wrapperId;
   private Encoder encoder;
   private Wrapper wrapper;

   private boolean isKey;
   private Transcoder transcoder;

   private DataConversion(Class<? extends Encoder> encoderClass, Class<? extends Wrapper> wrapperClass,
                          MediaType requestMediaType, MediaType storageMediaType, boolean isKey) {
      this.encoderClass = encoderClass;
      this.wrapperClass = wrapperClass;
      this.requestMediaType = requestMediaType;
      this.storageMediaType = storageMediaType;
      this.isKey = isKey;
   }

   /**
    * Used for de-serialization
    */
   private DataConversion(Short encoderId, Byte wrapperId, MediaType requestMediaType, MediaType storageMediaType,
                          boolean isKey) {
      this.encoderId = encoderId;
      this.wrapperId = wrapperId;
      this.requestMediaType = requestMediaType;
      this.storageMediaType = storageMediaType;
      this.isKey = isKey;
   }

   private DataConversion(Encoder encoder, Wrapper wrapper, boolean isKey) {
      this.encoder = encoder;
      this.wrapper = wrapper;
      this.encoderClass = encoder.getClass();
      this.wrapperClass = wrapper.getClass();
      this.isKey = isKey;
   }

   public DataConversion withRequestMediaType(MediaType requestMediaType) {
      if (Objects.equals(this.requestMediaType, requestMediaType)) return this;
      return new DataConversion(this.encoderClass, this.wrapperClass, requestMediaType, this.storageMediaType,
            this.isKey);
   }

   public DataConversion withEncoding(Class<? extends Encoder> encoderClass) {
      if (Objects.equals(this.encoderClass, encoderClass)) return this;
      return new DataConversion(encoderClass, this.wrapperClass, this.requestMediaType, this.storageMediaType,
            this.isKey);
   }

   public DataConversion withWrapping(Class<? extends Wrapper> wrapperClass) {
      if (Objects.equals(this.wrapperClass, wrapperClass)) return this;
      return new DataConversion(this.encoderClass, wrapperClass, this.requestMediaType, this.storageMediaType,
            this.isKey);
   }

   public void overrideWrapper(Class<? extends Wrapper> newWrapper, ComponentRegistry cr) {
      this.wrapper = null;
      this.wrapperClass = newWrapper;
      cr.wireDependencies(this);
   }

   /**
    * Obtain the configured {@link MediaType} for this instance, or assume sensible defaults.
    */
   private MediaType getStorageMediaType(Configuration configuration, boolean embeddedMode) {
      EncodingConfiguration encodingConfiguration = configuration.encoding();
      ContentTypeConfiguration contentTypeConfiguration = isKey ? encodingConfiguration.keyDataType() : encodingConfiguration.valueDataType();
      // If explicitly configured, use the value provided
      if (contentTypeConfiguration.isMediaTypeChanged()) {
         return contentTypeConfiguration.mediaType();
      }
      // Compatibility mode assumes application/x-java-object
      if (configuration.compatibility().enabled()) {
         return MediaType.APPLICATION_OBJECT;
      }
      // Indexed caches started by the server will assume application/protostream as storage media type
      if (!embeddedMode && configuration.indexing().index().isEnabled() && contentTypeConfiguration.mediaType() == null) {
         return MediaType.APPLICATION_PROTOSTREAM;
      }
      // Otherwise don't guess the media type, preventing transcoding.
      return null;
   }

   @Inject
   public void injectDependencies(GlobalConfiguration gcr, EncoderRegistry encoderRegistry, Configuration configuration) {
      boolean embeddedMode = Configurations.isEmbeddedMode(gcr);
      CompatibilityModeConfiguration compatibility = configuration.compatibility();
      boolean compat = compatibility.enabled();

      StorageType storageType = configuration.memory().storageType();
      boolean offheap = storageType == StorageType.OFF_HEAP;
      boolean binary = storageType == StorageType.BINARY;
      boolean isEncodingEmpty = encoderClass == null && encoderId == null && encoder == null;
      if (isEncodingEmpty) {
         encoderClass = IdentityEncoder.class;
         if (offheap) {
            if (embeddedMode) {
               encoderClass = GlobalMarshallerEncoder.class;
            }
         }
         if (binary) {
            encoderClass = BinaryEncoder.class;
         }
         if (compat) {
            this.encoderClass = embeddedMode ? IdentityEncoder.class : CompatModeEncoder.class;
         }
      }
      this.lookupWrapper(encoderRegistry);
      this.lookupEncoder(encoderRegistry, compatibility, gcr.classLoader());
      this.storageMediaType = getStorageMediaType(configuration, embeddedMode);
      this.lookupTranscoder(encoderRegistry);
   }

   private void lookupTranscoder(EncoderRegistry encoderRegistry) {
      boolean needsTranscoding = storageMediaType != null && requestMediaType != null && !requestMediaType.equals(storageMediaType);
      if (needsTranscoding) {
         Transcoder directTranscoder = null;
         if (encoder.getStorageFormat() != null) {
            try {
               directTranscoder = encoderRegistry.getTranscoder(requestMediaType, encoder.getStorageFormat());
            } catch (EncodingException ignored) {
            }
         }
         if (directTranscoder != null) {
            if (encoder.getStorageFormat().equals(MediaType.APPLICATION_OBJECT)) {
               encoder = IdentityEncoder.INSTANCE;
            }
            transcoder = directTranscoder;
         } else {
            transcoder = encoderRegistry.getTranscoder(requestMediaType, storageMediaType);
         }
      }
   }

   private void lookupEncoder(EncoderRegistry encoderRegistry, CompatibilityModeConfiguration compatConfig, ClassLoader classLoader) {
      if (!compatConfig.enabled()) {
         this.encoder = encoderRegistry.getEncoder(encoderClass, encoderId);
      } else {
         if (encoderClass == null || encoderClass.equals(CompatModeEncoder.class)) {
            this.encoderClass = CompatModeEncoder.class;
            Marshaller compatMarshaller = compatConfig.marshaller();
            this.encoder = new CompatModeEncoder(compatMarshaller, classLoader);
         } else {
            this.encoder = encoderRegistry.getEncoder(encoderClass, encoderId);
         }
      }
   }

   private void lookupWrapper(EncoderRegistry encoderRegistry) {
      this.wrapper = encoderRegistry.getWrapper(wrapperClass, wrapperId);
   }

   public Object fromStorage(Object stored) {
      if (stored == null) return null;
      Object fromStorage = encoder.fromStorage(wrapper.unwrap(stored));
      return transcoder == null ? fromStorage : transcoder.transcode(fromStorage, storageMediaType, requestMediaType);
   }

   public Object toStorage(Object toStore) {
      if (toStore == null) return null;
      toStore = transcoder == null ? toStore : transcoder.transcode(toStore, requestMediaType, storageMediaType);
      return wrapper.wrap(encoder.toStorage(toStore));
   }

   public Object extractIndexable(Object stored) {
      if (stored == null) return null;
      if (encoder.isStorageFormatFilterable()) {
         return wrapper.isFilterable() ? stored : wrapper.unwrap(stored);
      }
      return encoder.fromStorage(wrapper.isFilterable() ? stored : wrapper.unwrap(stored));
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
      return Objects.equals(encoder, that.encoder) &&
            Objects.equals(wrapper, that.wrapper) &&
            Objects.equals(transcoder, that.transcoder);
   }

   @Override
   public String toString() {
      return "DataConversion{" +
            "encoderClass=" + encoderClass +
            ", wrapperClass=" + wrapperClass +
            ", requestMediaType=" + requestMediaType +
            ", storageMediaType=" + storageMediaType +
            ", encoderId=" + encoderId +
            ", wrapperId=" + wrapperId +
            ", encoder=" + encoder +
            ", wrapper=" + wrapper +
            ", isKey=" + isKey +
            ", transcoder=" + transcoder +
            '}';
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

      private boolean isDefault(DataConversion dataConversion) {
         return dataConversion.isKey && dataConversion.equals(DEFAULT_KEY) ||
               !dataConversion.isKey && dataConversion.equals(DEFAULT_VALUE);
      }

      @Override
      public void writeObject(ObjectOutput output, DataConversion dataConversion) throws IOException {
         if (isDefault(dataConversion)) {
            output.writeByte(1);
         } else {
            byte flags = 0;
            if (dataConversion.isKey) flags = (byte) (flags | 2);
            output.writeByte(flags);
            output.writeShort(dataConversion.encoder.id());
            output.writeByte(dataConversion.wrapper.id());
            output.writeObject(dataConversion.requestMediaType);
         }
      }

      @Override
      public DataConversion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         byte flags = input.readByte();
         boolean isKey = ((flags & 2) == 2);
         if (((flags & 1) == 1))
            return isKey ? DEFAULT_KEY : DEFAULT_VALUE;

         short encoderId = input.readShort();
         byte wrapperId = input.readByte();
         MediaType requestMediaType = (MediaType) input.readObject();
         return new DataConversion(encoderId, wrapperId, requestMediaType, null, isKey);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_CONVERSION;
      }
   }

   public static DataConversion newKeyDataConversion(Class<? extends Encoder> encoderClass, Class<? extends Wrapper> wrapperClass, MediaType requestType) {
      return new DataConversion(encoderClass, wrapperClass, requestType, null, true);
   }

   public static DataConversion newValueDataConversion(Class<? extends Encoder> encoderClass, Class<? extends Wrapper> wrapperClass, MediaType requestType) {
      return new DataConversion(encoderClass, wrapperClass, requestType, null, false);
   }

}
