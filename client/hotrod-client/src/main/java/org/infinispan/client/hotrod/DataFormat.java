package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;
import static org.infinispan.client.hotrod.marshall.MarshallerUtil.obj2bytes;

import java.util.List;

import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.IdentityMarshaller;
import org.infinispan.commons.marshall.Marshaller;

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

   private DataFormat(MediaType keyType, MediaType valueType, Marshaller keyMarshaller, Marshaller valueMarshaller) {
      this.keyType = keyType;
      this.valueType = valueType;
      this.keyMarshaller = keyMarshaller;
      this.valueMarshaller = valueMarshaller;
   }

   public DataFormat withoutValueType() {
      return new DataFormat(keyType, null, keyMarshaller, null);
   }

   public MediaType getKeyType() {
      return keyType;
   }

   public MediaType getValueType() {
      return valueType;
   }

   public void initialize(RemoteCacheManager remoteCacheManager) {
      this.marshallerRegistry = remoteCacheManager.getMarshallerRegistry();
      this.defaultMarshaller = remoteCacheManager.getMarshaller();
   }

   private Marshaller resolveValueMarshaller() {
      if (valueMarshaller != null) return valueMarshaller;
      if (valueType == null) return defaultMarshaller;

      Marshaller forValueType = marshallerRegistry.getMarshaller(valueType);
      if (forValueType != null) return forValueType;
      log.debugf("No marshaller registered for %s, using no-op marshaller", valueType);

      return IdentityMarshaller.INSTANCE;
   }

   private Marshaller resolveKeyMarshaller() {
      if (keyMarshaller != null) return keyMarshaller;
      if (keyType == null) return defaultMarshaller;

      Marshaller forKeyType = marshallerRegistry.getMarshaller(keyType);
      if (forKeyType != null) return forKeyType;
      log.debugf("No marshaller registered for %s, using no-op marshaller", keyType);

      return IdentityMarshaller.INSTANCE;
   }

   public boolean hasCustomFormat() {
      return keyType != null || valueType != null;
   }

   public byte[] keyToBytes(Object key, int estimateKeySize, int estimateValueSize) {
      Marshaller keyMarshaller = resolveKeyMarshaller();
      return obj2bytes(keyMarshaller, key, true, estimateKeySize, estimateValueSize);
   }

   public byte[] valueToBytes(Object value, int estimateKeySize, int estimateValueSize) {
      Marshaller valueMarshaller = resolveValueMarshaller();
      return obj2bytes(valueMarshaller, value, false, estimateKeySize, estimateValueSize);
   }

   public <T> T keyToObj(byte[] bytes, short status, List<String> whitelist) {
      Marshaller keyMarshaller = resolveKeyMarshaller();
      return bytes2obj(keyMarshaller, bytes, status, whitelist);
   }

   public <T> T valueToObj(byte[] bytes, short status, List<String> whitelist) {
      Marshaller valueMarshaller = resolveValueMarshaller();
      return bytes2obj(valueMarshaller, bytes, status, whitelist);
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
