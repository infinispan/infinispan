package org.infinispan.encoding.impl;

import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.EncodingConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.registry.InternalCacheRegistry;

/**
 * Key/value storage information (storage media type and wrapping).
 *
 * @author Dan Berindei
 * @since 11
 */
@Scope(Scopes.NAMED_CACHE)
public class StorageConfigurationManager {
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;
   private MediaType keyStorageMediaType;
   private MediaType valueStorageMediaType;

   public Wrapper getKeyWrapper() {
      return keyWrapper;
   }

   public Wrapper getValueWrapper() {
      return valueWrapper;
   }

   public Wrapper getWrapper(boolean isKey) {
      return isKey ? keyWrapper : valueWrapper;
   }

   public void overrideWrapper(Wrapper keyWrapper, Wrapper valueWrapper) {
      // TODO If the remote query module could override the wrapper at injection time,
      //  DataConversion wouldn't need to look up the wrapper every time
      this.keyWrapper = keyWrapper;
      this.valueWrapper = valueWrapper;
   }

   public MediaType getKeyStorageMediaType() {
      return keyStorageMediaType;
   }

   public MediaType getValueStorageMediaType() {
      return valueStorageMediaType;
   }

   public MediaType getStorageMediaType(boolean isKey) {
      return isKey ? keyStorageMediaType : valueStorageMediaType;
   }

   public StorageConfigurationManager() {
      keyWrapper = ByteArrayWrapper.INSTANCE;
      valueWrapper = ByteArrayWrapper.INSTANCE;
   }

   @Inject
   void injectDependencies(@ComponentName(KnownComponentNames.PERSISTENCE_MARSHALLER) PersistenceMarshaller persistenceMarshaller,
                           @ComponentName(KnownComponentNames.CACHE_NAME) String cacheName,
                           InternalCacheRegistry icr, GlobalConfiguration gcr,
                           EncoderRegistry encoderRegistry, Configuration configuration) {
      boolean internalCache = icr.isInternalCache(cacheName);
      boolean embeddedMode = Configurations.isEmbeddedMode(gcr);
      this.keyStorageMediaType = getStorageMediaType(configuration, embeddedMode, internalCache, persistenceMarshaller,
                                                     true);
      this.valueStorageMediaType = getStorageMediaType(configuration, embeddedMode, internalCache, persistenceMarshaller,
                                                     false);
   }

   private MediaType getStorageMediaType(Configuration configuration, boolean embeddedMode, boolean internalCache,
                                         PersistenceMarshaller persistenceMarshaller, boolean isKey) {
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
}
