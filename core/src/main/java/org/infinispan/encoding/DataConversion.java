package org.infinispan.encoding;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.encoding.impl.DataConversionInternal;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoReserved;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.Objects;

/**
 * Handle conversions for Keys or values.
 *
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.DATA_CONVERSION)
@ProtoReserved(numbers = {2,3}, names = {"encoderId", "wrapperId"})
@Scope(Scopes.NONE)
public final class DataConversion {

   private final MediaType requestMediaType;
   private final boolean isKey;

   private transient MediaType storageMediaType;
   private transient Transcoder transcoder;
   private transient EncoderRegistry encoderRegistry;
   private transient StorageConfigurationManager storageConfigurationManager;

   private DataConversion(MediaType requestMediaType, boolean isKey) {
      this.requestMediaType = requestMediaType;
      this.isKey = isKey;
   }

   public DataConversion(boolean isKey) {
      this.isKey = isKey;
      this.storageMediaType = MediaType.APPLICATION_OBJECT;
      this.requestMediaType = MediaType.APPLICATION_OBJECT;
   }

   @ProtoFactory
   static DataConversion protoFactory(boolean isKey, MediaType mediaType) {
      return new DataConversion(mediaType, isKey);
   }

   @ProtoField(1)
   boolean getIsKey() {
      return isKey;
   }


   @ProtoField(4)
   MediaType getMediaType() {
      return requestMediaType;
   }

   public DataConversion withRequestMediaType(MediaType requestMediaType) {
      if (Objects.equals(this.requestMediaType, requestMediaType)) return this;
      return new DataConversion(requestMediaType, this.isKey);
   }

   @Inject
   void injectDependencies(StorageConfigurationManager storageConfigurationManager, EncoderRegistry encoderRegistry) {
      if (this == DataConversionInternal.IDENTITY_KEY || this == DataConversionInternal.IDENTITY_VALUE) {
         return;
      }
      this.storageMediaType = storageConfigurationManager.getStorageMediaType(isKey);
      this.encoderRegistry = encoderRegistry;
      this.storageConfigurationManager = storageConfigurationManager;
      this.lookupTranscoder();
   }

   private void lookupTranscoder() {
      boolean needsTranscoding = storageMediaType != null && requestMediaType != null && !requestMediaType.matchesAll() && !requestMediaType.equals(storageMediaType);
      if (needsTranscoding) {
            transcoder = encoderRegistry.getTranscoder(requestMediaType, storageMediaType);
      }
   }

   public Object fromStorage(Object stored) {
      if (stored == null) return null;
      Object fromStorage = unwrap(stored);
      return transcoder == null ? fromStorage : transcoder.transcode(fromStorage, storageMediaType, requestMediaType);
   }

   public Object toStorage(Object toStore) {
      if (toStore == null) return null;
      toStore = transcoder == null ? toStore : transcoder.transcode(toStore, requestMediaType, storageMediaType);
      return wrap(toStore);
   }

   /**
    * Convert the stored object in a format suitable to be indexed.
    */
   public Object extractIndexable(Object stored, boolean javaEmbeddedEntities) {
      if (stored == null) return null;

      // Keys are indexed as stored, without the wrapper
      if (isKey) return unwrap(stored);

      Wrapper wrapper = storageConfigurationManager.getWrapper(false);
      if (wrapper.isFilterable() && !javaEmbeddedEntities) {
         // If the value wrapper is indexable, return the already wrapped value or wrap it otherwise
         return stored.getClass() == wrapper.getClass() ? stored : wrapper.wrap(stored);
      }

      // Otherwise convert to the request format
      Object unencoded = unwrap(stored);
      return transcoder == null ? unencoded : transcoder.transcode(unencoded, storageMediaType, requestMediaType);
   }

   public MediaType getRequestMediaType() {
      return requestMediaType;
   }

   public MediaType getStorageMediaType() {
      return storageMediaType;
   }

   public Object wrap(Object o) {
      if (storageConfigurationManager != null) {
         return storageConfigurationManager.getWrapper(isKey).wrap(o);
      } else {
         return o;
      }
   }

   public Object unwrap(Object o) {
      if (storageConfigurationManager != null) {
         return storageConfigurationManager.getWrapper(isKey).unwrap(o);
      } else {
         return o;
      }
   }


   /**
    * @return A new instance with request type {@link MediaType#APPLICATION_OBJECT}.
    * @since 11.0
    */
   public static DataConversion newKeyDataConversion() {
      return new DataConversion(MediaType.APPLICATION_OBJECT, true);
   }

   /**
    * @return A new instance with request type {@link MediaType#APPLICATION_OBJECT}.
    * @since 11.0
    */
   public static DataConversion newValueDataConversion() {
      return new DataConversion(MediaType.APPLICATION_OBJECT, false);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DataConversion that = (DataConversion) o;
      return isKey == that.isKey &&
            Objects.equals(transcoder, that.transcoder) &&
            Objects.equals(requestMediaType, that.requestMediaType);
   }

   @Override
   public String toString() {
      return "DataConversion@" + System.identityHashCode(this) + "{" +
            "requestMediaType=" + requestMediaType +
            ", storageMediaType=" + storageMediaType +
            ", isKey=" + isKey +
            ", transcoder=" + transcoder +
            '}';
   }

   @Override
   public int hashCode() {
      return Objects.hash(requestMediaType, isKey);
   }
}
