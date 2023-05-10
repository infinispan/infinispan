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
import org.infinispan.commons.util.Util;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;

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
   private transient Wrapper customWrapper;
   private transient Transcoder transcoder;
   private transient EncoderRegistry encoderRegistry;
   private transient StorageConfigurationManager storageConfigurationManager;

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
      this.customWrapper = wrapper;
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
      return new DataConversion(this.encoderClass, this.wrapperClass, requestMediaType, this.isKey);
   }

   /**
    * @deprecated Since 12.1, to be removed in a future version.
    */
   @Deprecated
   public DataConversion withEncoding(Class<? extends Encoder> encoderClass) {
      if (Objects.equals(this.encoderClass, encoderClass)) return this;
      return new DataConversion(encoderClass, this.wrapperClass, this.requestMediaType, this.isKey);
   }

   /**
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   @Deprecated
   public DataConversion withWrapping(Class<? extends Wrapper> wrapperClass) {
      if (Objects.equals(this.wrapperClass, wrapperClass)) return this;
      return new DataConversion(this.encoderClass, wrapperClass, this.requestMediaType, this.isKey);
   }

   /**
    * @deprecated Since 11.0, will be removed with no replacement
    */
   @Deprecated
   public void overrideWrapper(Class<? extends Wrapper> newWrapper, ComponentRegistry cr) {
      this.customWrapper = null;
      this.wrapperClass = newWrapper;
      cr.wireDependencies(this);
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
   void injectDependencies(StorageConfigurationManager storageConfigurationManager, EncoderRegistry encoderRegistry) {
      if (this.encoder != null && this.customWrapper != null) {
         // This must be one of the static encoders, we can't inject any component in it
         return;
      }
      this.storageMediaType = storageConfigurationManager.getStorageMediaType(isKey);

      this.encoderRegistry = encoderRegistry;
      this.storageConfigurationManager = storageConfigurationManager;
      this.customWrapper = encoderRegistry.getWrapper(wrapperClass, wrapperId);
      this.lookupEncoder();
      this.lookupTranscoder();
   }

   private void lookupEncoder() {
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

   public Object fromStorage(Object stored) {
      if (stored == null) return null;
      Object fromStorage = encoder.fromStorage(getWrapper().unwrap(stored));
      return transcoder == null ? fromStorage : transcoder.transcode(fromStorage, storageMediaType, requestMediaType);
   }

   public Object toStorage(Object toStore) {
      if (toStore == null) return null;
      toStore = transcoder == null ? toStore : transcoder.transcode(toStore, requestMediaType, storageMediaType);
      return getWrapper().wrap(encoder.toStorage(toStore));
   }

   /**
    * Convert the stored object in a format suitable to be indexed.
    */
   public Object extractIndexable(Object stored) {
      if (stored == null) return null;

      // Keys are indexed as stored, without the wrapper
      Wrapper wrapper = getWrapper();
      if (isKey) return wrapper.unwrap(stored);

      if (wrapper.isFilterable()) {
         // If the value wrapper is indexable, return the already wrapped value or wrap it otherwise
         return stored.getClass().equals(wrapperClass) ? stored : wrapper.wrap(stored);
      }

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

   /**
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   @Deprecated
   public Wrapper getWrapper() {
      if (customWrapper != null)
         return customWrapper;

      return storageConfigurationManager.getWrapper(isKey);
   }

   public Class<? extends Encoder> getEncoderClass() {
      return encoderClass;
   }

   /**
    * @deprecated Since 11.0. To be removed in 14.0, with no replacement.
    */
   @Deprecated
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
            Objects.equals(customWrapper, that.customWrapper) &&
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
            ", wrapper=" + customWrapper +
            ", isKey=" + isKey +
            ", transcoder=" + transcoder +
            '}';
   }

   @Override
   public int hashCode() {
      return Objects.hash(encoderClass, wrapperClass, requestMediaType, isKey);
   }

   /**
    * @return A new instance with an {@link IdentityEncoder} and request type {@link MediaType#APPLICATION_OBJECT}.
    * @since 11.0
    */
   public static DataConversion newKeyDataConversion() {
      return new DataConversion(IdentityEncoder.class, null, MediaType.APPLICATION_OBJECT, true);
   }

   /**
    * @return A new instance with an {@link IdentityEncoder} and request type {@link MediaType#APPLICATION_OBJECT}.
    * @since 11.0
    */
   public static DataConversion newValueDataConversion() {
      return new DataConversion(IdentityEncoder.class, null, MediaType.APPLICATION_OBJECT, false);
   }

   /**
    * @deprecated Since 11.0. To be removed in 14.0. Replaced by {@link #newKeyDataConversion()}.
    */
   @Deprecated
   public static DataConversion newKeyDataConversion(Class<? extends Encoder> encoderClass,
                                                     Class<? extends Wrapper> wrapperClass) {
      return new DataConversion(encoderClass, wrapperClass, MediaType.APPLICATION_OBJECT, true);
   }

   /**
    * @deprecated Since 11.0. To be removed in 14.0. Replaced by {@link #newValueDataConversion()}.
    */
   @Deprecated
   public static DataConversion newValueDataConversion(Class<? extends Encoder> encoderClass,
                                                       Class<? extends Wrapper> wrapperClass) {
      return new DataConversion(encoderClass, wrapperClass, MediaType.APPLICATION_OBJECT, false);
   }

   public static void writeTo(ObjectOutput output, DataConversion dataConversion) throws IOException {
      byte flags = 0;
      if (dataConversion.isKey) flags = (byte) (flags | 2);
      output.writeByte(flags);
      output.writeShort(dataConversion.encoder.id());
      if (dataConversion.customWrapper != null) {
         output.writeByte(dataConversion.customWrapper.id());
      } else {
         output.writeByte(WrapperIds.NO_WRAPPER);
      }
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
