package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;
import static org.infinispan.client.hotrod.marshall.MarshallerUtil.obj2bytes;

import org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.AdaptiveBufferSizePredictor;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.Util;

/**
 * Defines data format for keys and values during Hot Rod client requests.
 *
 * @since 9.3
 */
public final class DataFormat {

   private static final Log log = LogFactory.getLog(DataFormat.class, Log.class);

   private final MediaType keyType;
   private final MediaType valueType;
   private final Marshaller keyMarshaller;
   private final Marshaller valueMarshaller;

   private MarshallerRegistry marshallerRegistry;
   private Marshaller defaultMarshaller;
   private boolean isObjectStorage;

   private final BufferSizePredictor keySizePredictor = new AdaptiveBufferSizePredictor();
   private final BufferSizePredictor valueSizePredictor = new AdaptiveBufferSizePredictor();


   private DataFormat(MediaType keyType, MediaType valueType, Marshaller keyMarshaller, Marshaller valueMarshaller) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
   }

   public DataFormat withoutValueType() {
      DataFormat dataFormat = new DataFormat(keyType, null, keyMarshaller, null);
      dataFormat.marshallerRegistry = this.marshallerRegistry;
      dataFormat.defaultMarshaller = this.defaultMarshaller;
      dataFormat.isObjectStorage = this.isObjectStorage;
      return dataFormat;
   }

   public MediaType getKeyType() {
      if (keyType != null) return keyType;
      Marshaller marshaller = resolveKeyMarshaller();
      return marshaller == null ? null : marshaller.mediaType();
   }

   public MediaType getValueType() {
      if (valueType != null) return valueType;
      Marshaller marshaller = resolveValueMarshaller();
      return marshaller == null ? null : marshaller.mediaType();
   }

   /**
    * @deprecated Replaced by {@link #initialize(RemoteCacheManager, String, boolean)}.
    */
   @Deprecated
   public void initialize(RemoteCacheManager remoteCacheManager, boolean serverObjectStorage) {
      this.marshallerRegistry = remoteCacheManager.getMarshallerRegistry();
      this.defaultMarshaller = remoteCacheManager.getMarshaller();
      this.isObjectStorage = serverObjectStorage;
   }

   public void initialize(RemoteCacheManager remoteCacheManager, String cacheName, boolean serverObjectStorage) {
      this.marshallerRegistry = remoteCacheManager.getMarshallerRegistry();
      this.isObjectStorage = serverObjectStorage;
      this.defaultMarshaller = remoteCacheManager.getMarshaller();
      RemoteCacheConfiguration remoteCacheConfiguration = remoteCacheManager.getConfiguration().remoteCaches().get(cacheName);
      if (remoteCacheConfiguration != null) {
         Marshaller cacheMarshaller = remoteCacheConfiguration.marshaller();
         if (cacheMarshaller != null) {
            defaultMarshaller = cacheMarshaller;
         } else {
            Class<? extends Marshaller> marshallerClass = remoteCacheConfiguration.marshallerClass();
            if (marshallerClass != null) {
               Marshaller registryMarshaller = marshallerRegistry.getMarshaller(marshallerClass);
               defaultMarshaller = registryMarshaller != null ? registryMarshaller : Util.getInstance(marshallerClass);
            }
         }
      }
   }

   private Marshaller resolveValueMarshaller() {
      if (valueMarshaller != null) return valueMarshaller;
      if (valueType == null) return defaultMarshaller;

      Marshaller forValueType = marshallerRegistry.getMarshaller(valueType);
      if (forValueType != null) return forValueType;
      log.debugf("No marshaller registered for %s, using no-op marshaller", valueType);

      return IdentityMarshaller.INSTANCE;
   }

   public boolean isObjectStorage() {
      return isObjectStorage;
   }

   private Marshaller resolveKeyMarshaller() {
      if (keyMarshaller != null) return keyMarshaller;
      if (keyType == null) return defaultMarshaller;

      Marshaller forKeyType = marshallerRegistry.getMarshaller(keyType);
      if (forKeyType != null) return forKeyType;
      log.debugf("No marshaller registered for %s, using no-op marshaller", keyType);

      return IdentityMarshaller.INSTANCE;
   }

   /**
    * @deprecated Since 12.0, will be removed in 15.0
    */
   @Deprecated
   public byte[] keyToBytes(Object key, int estimateKeySize, int estimateValueSize) {
      return keyToBytes(key);
   }

   public byte[] keyToBytes(Object key) {
      Marshaller keyMarshaller = resolveKeyMarshaller();
      return obj2bytes(keyMarshaller, key, keySizePredictor);
   }

   /**
    * @deprecated Since 12.0, will be removed in 15.0
    */
   @Deprecated
   public byte[] valueToBytes(Object value, int estimateKeySize, int estimateValueSize) {
      return valueToBytes(value);
   }

   public byte[] valueToBytes(Object value) {
      Marshaller valueMarshaller = resolveValueMarshaller();
      return obj2bytes(valueMarshaller, value, valueSizePredictor);
   }

   public <T> T keyToObj(byte[] bytes, ClassAllowList allowList) {
      Marshaller keyMarshaller = resolveKeyMarshaller();
      return bytes2obj(keyMarshaller, bytes, isObjectStorage, allowList);
   }

   public <T> T valueToObj(byte[] bytes, ClassAllowList allowList) {
      Marshaller valueMarshaller = resolveValueMarshaller();
      return bytes2obj(valueMarshaller, bytes, isObjectStorage, allowList);
   }

   @Override
   public String toString() {
      return "DataFormat{" +
            "keyType=" + keyType +
            ", valueType=" + valueType +
            ", keyMarshaller=" + keyMarshaller +
            ", valueMarshaller=" + valueMarshaller +
            ", marshallerRegistry=" + marshallerRegistry +
            ", defaultMarshaller=" + defaultMarshaller +
            '}';
   }

   public static Builder builder() {
      return new Builder();
   }

   public static class Builder {
      private MediaType keyType;
      private MediaType valueType;
      private Marshaller valueMarshaller;
      private Marshaller keyMarshaller;

      public Builder from(DataFormat dataFormat) {
         this.keyType = dataFormat.keyType;
         this.valueType = dataFormat.valueType;
         this.keyMarshaller = dataFormat.keyMarshaller;
         this.valueMarshaller = dataFormat.valueMarshaller;
         return this;
      }

      public Builder valueMarshaller(Marshaller valueMarshaller) {
         this.valueMarshaller = valueMarshaller;
         return this;
      }

      public Builder keyMarshaller(Marshaller keyMarshaller) {
         this.keyMarshaller = keyMarshaller;
         return this;
      }

      public Builder keyType(MediaType keyType) {
         this.keyType = keyType;
         return this;
      }

      public Builder valueType(MediaType valueType) {
         this.valueType = valueType;
         return this;
      }

      public DataFormat build() {
         return new DataFormat(keyType, valueType, keyMarshaller, valueMarshaller);
      }

   }
}
