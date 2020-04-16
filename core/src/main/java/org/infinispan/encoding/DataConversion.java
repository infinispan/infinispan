package org.infinispan.encoding;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.EncoderIds;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.dataconversion.WrapperIds;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.registry.InternalCacheRegistry;

/**
 * Handle conversions for Keys or values.
 *
 * @since 9.2
 */
@Scope(Scopes.NONE)
public final class DataConversion {

   /**
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   @Deprecated
   public static final DataConversion DEFAULT_KEY = new DataConversion(IdentityEncoder.INSTANCE, ByteArrayWrapper.INSTANCE, true);
   /**
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   @Deprecated
   public static final DataConversion DEFAULT_VALUE = new DataConversion(IdentityEncoder.INSTANCE, ByteArrayWrapper.INSTANCE, false);
   /**
    * @deprecated Since 11.0. To be removed in 14.0. For internal use only.
    */
   @Deprecated
   public static final DataConversion IDENTITY_KEY = new DataConversion(IdentityEncoder.INSTANCE, IdentityWrapper.INSTANCE, true);
   /**
    * @deprecated Since 11.0. To be removed in 14.0. For internal use only.
    */
   @Deprecated
   public static final DataConversion IDENTITY_VALUE = new DataConversion(IdentityEncoder.INSTANCE, IdentityWrapper.INSTANCE, false);

   // On the origin node the conversion is initialized with the encoder/wrapper classes, on remote nodes with the ids
   private final transient Class<? extends Encoder> encoderClass;
   // TODO Make final after removing overrideWrapper()
   private transient Class<? extends Wrapper> wrapperClass;
   private final short encoderId;
   private final byte wrapperId;
   private final MediaType requestMediaType;
   private final boolean isKey;

   private transient MediaType storageMediaType;
   private transient Encoder encoder;
   private transient Wrapper wrapper;
   private transient Transcoder transcoder;
   private transient EncoderRegistry encoderRegistry;

   private DataConversion(Class<? extends Encoder> encoderClass, Class<? extends Wrapper> wrapperClass,
                          MediaType requestMediaType, boolean isKey) {
      this.encoderClass = encoderClass;
      this.wrapperClass = wrapperClass;
      this.requestMediaType = requestMediaType;
      this.isKey = isKey;
      this.encoderId = EncoderIds.NO_ENCODER;
      this.wrapperId = WrapperIds.NO_WRAPPER;
   }

   /**
    * Used for de-serialization
    */
   private DataConversion(Short encoderId, Byte wrapperId, MediaType requestMediaType, boolean isKey) {
      this.encoderId = encoderId;
      this.wrapperId = wrapperId;
      this.requestMediaType = requestMediaType;
      this.isKey = isKey;
      this.encoderClass = null;
      this.wrapperClass = null;
   }

   private DataConversion(Encoder encoder, Wrapper wrapper, boolean isKey) {
      this.encoder = encoder;
      this.wrapper = wrapper;
      this.encoderClass = encoder.getClass();
      this.wrapperClass = wrapper.getClass();
      this.isKey = isKey;
      this.storageMediaType = MediaType.APPLICATION_OBJECT;
      this.requestMediaType = MediaType.APPLICATION_OBJECT;
      encoderId = EncoderIds.NO_ENCODER;
      wrapperId = WrapperIds.NO_WRAPPER;
   }

   public DataConversion withRequestMediaType(MediaType requestMediaType) {
      if (Objects.equals(this.requestMediaType, requestMediaType)) return this;
      return new DataConversion(null, this.wrapperClass, requestMediaType, this.isKey);
   }

   public DataConversion withEncoding(Class<? extends Encoder> encoderClass) {
      if (Objects.equals(this.encoderClass, encoderClass)) return this;
      return new DataConversion(encoderClass, this.wrapperClass, this.requestMediaType, this.isKey);
   }

   public DataConversion withWrapping(Class<? extends Wrapper> wrapperClass) {
      if (Objects.equals(this.wrapperClass, wrapperClass)) return this;
      return new DataConversion(this.encoderClass, wrapperClass, this.requestMediaType, this.isKey);
   }

   /**
    * @deprecated Since 11.0, will be removed with no replacement
    */
   @Deprecated
   public void overrideWrapper(Class<? extends Wrapper> newWrapper, ComponentRegistry cr) {
      this.wrapper = null;
      this.wrapperClass = newWrapper;
      cr.wireDependencies(this);
   }

   /**
    * Obtain the configured {@link MediaType} for this instance, or assume sensible defaults.
    */
   private MediaType getStorageMediaType(Configuration configuration, boolean embeddedMode, boolean internalCache, PersistenceMarshaller persistenceMarshaller) {
      EncodingConfiguration encodingConfiguration = configuration.encoding();
      ContentTypeConfiguration contentTypeConfiguration = isKey ? encodingConfiguration.keyDataType() : encodingConfiguration.valueDataType();
      Marshaller userMarshaller = persistenceMarshaller.getUserMarshaller();
      MediaType mediaType = userMarshaller.mediaType();
      boolean heap = configuration.memory().storageType() == StorageType.OBJECT;
      // If explicitly configured, use the value provided
      if (contentTypeConfiguration.isMediaTypeChanged()) {
         return contentTypeConfiguration.mediaType();
      }
      // Indexed caches started by the server will assume application/protostream as storage media type
      if (!embeddedMode && configuration.indexing().enabled() && contentTypeConfiguration.mediaType() == null) {
         return MediaType.APPLICATION_PROTOSTREAM;
      }
      if (internalCache) return MediaType.APPLICATION_OBJECT;

      if (embeddedMode) {
         return heap ? MediaType.APPLICATION_OBJECT : mediaType;
      }

      return MediaType.APPLICATION_UNKNOWN;
   }

   /**
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   public boolean isConversionSupported(MediaType mediaType) {
      return storageMediaType == null || encoderRegistry.isConversionSupported(storageMediaType, mediaType);
   }

   /**
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   public Object convert(Object o, MediaType from, MediaType to) {
      if (o == null) return null;
      if (encoderRegistry == null) return o;
      Transcoder transcoder = encoderRegistry.getTranscoder(from, to);
      return transcoder.transcode(o, from, to);
   }

   /**
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   public Object convertToRequestFormat(Object o, MediaType contentType) {
      if (o == null) return null;
      if (requestMediaType == null) return fromStorage(o);
      Transcoder transcoder = encoderRegistry.getTranscoder(contentType, requestMediaType);
      return transcoder.transcode(o, contentType, requestMediaType);
   }

   @Inject
   public void injectDependencies(@ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER) PersistenceMarshaller persistenceMarshaller,
                                  @ComponentName(KnownComponentNames.CACHE_NAME) String cacheName,
                                  InternalCacheRegistry icr, GlobalConfiguration gcr,
                                  EncoderRegistry encoderRegistry, Configuration configuration) {
      this.encoderRegistry = encoderRegistry;
      if (this.encoder != null && this.wrapper != null) {
         // This must be one of the static encoders, we can't inject any component in it
         return;
      }
      boolean internalCache = icr.isInternalCache(cacheName);
      boolean embeddedMode = Configurations.isEmbeddedMode(gcr);
      this.storageMediaType = getStorageMediaType(configuration, embeddedMode, internalCache, persistenceMarshaller);

      lookupEncoder(encoderRegistry);
      this.lookupWrapper();
      this.lookupTranscoder();
   }

   private void lookupEncoder(EncoderRegistry encoderRegistry) {
      boolean isEncodingEmpty = encoderClass == null && encoderId == EncoderIds.NO_ENCODER;
      Class<? extends Encoder> actualEncoderClass = isEncodingEmpty ? IdentityEncoder.class : encoderClass;
      this.encoder = encoderRegistry.getEncoder(actualEncoderClass, encoderId);
   }

   private void lookupTranscoder() {
      boolean needsTranscoding = storageMediaType != null && requestMediaType != null && !requestMediaType.matchesAll() && !requestMediaType.equals(storageMediaType);
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

   private void lookupWrapper() {
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

   /**
    * Convert the stored object in a format suitable to be indexed.
    */
   public Object extractIndexable(Object stored) {
      if (stored == null) return null;

      // Keys are indexed as stored, without the wrapper
      if (isKey) return wrapper.unwrap(stored);

      // If the value wrapper is indexable, just use it
      if (wrapper.isFilterable()) return stored;

      // Otherwise convert to the request format
      Object unencoded = encoder.fromStorage(wrapper.unwrap(stored));
      return transcoder == null ? unencoded : transcoder.transcode(unencoded, storageMediaType, requestMediaType);
   }

   public MediaType getRequestMediaType() {
      return requestMediaType;
   }

   public MediaType getStorageMediaType() {
      return storageMediaType;
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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataConversion that = (DataConversion) o;
      return isKey == that.isKey &&
            Objects.equals(encoder, that.encoder) &&
            Objects.equals(wrapper, that.wrapper) &&
            Objects.equals(transcoder, that.transcoder) &&
            Objects.equals(requestMediaType, that.requestMediaType);
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
      return Objects.hash(encoderClass, wrapperClass, isKey);
   }

   public static DataConversion newKeyDataConversion(Class<? extends Encoder> encoderClass,
                                                     Class<? extends Wrapper> wrapperClass) {
      return new DataConversion(encoderClass, wrapperClass, MediaType.APPLICATION_OBJECT, true);
   }

   public static DataConversion newValueDataConversion(Class<? extends Encoder> encoderClass,
                                                       Class<? extends Wrapper> wrapperClass) {
      return new DataConversion(encoderClass, wrapperClass, MediaType.APPLICATION_OBJECT, false);
   }

   public static void writeTo(ObjectOutput output, DataConversion dataConversion) throws IOException {
      byte flags = 0;
      if (dataConversion.isKey) flags = (byte) (flags | 2);
      output.writeByte(flags);
      output.writeShort(dataConversion.encoder.id());
      output.writeByte(dataConversion.wrapper.id());
      output.writeObject(dataConversion.requestMediaType);
   }

   public static DataConversion readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      byte flags = input.readByte();
      boolean isKey = ((flags & 2) == 2);

      short encoderId = input.readShort();
      byte wrapperId = input.readByte();
      MediaType requestMediaType = (MediaType) input.readObject();
      return new DataConversion(encoderId, wrapperId, requestMediaType, isKey);
   }

   public static class Externalizer extends AbstractExternalizer<DataConversion> {

      @Override
      public Set<Class<? extends DataConversion>> getTypeClasses() {
         return Util.asSet(DataConversion.class);
      }

      @Override
      public void writeObject(ObjectOutput output, DataConversion dataConversion) throws IOException {
         writeTo(output, dataConversion);
      }

      @Override
      public DataConversion readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return readFrom(input);
      }

      @Override
      public Integer getId() {
         return Ids.DATA_CONVERSION;
      }
   }

}
