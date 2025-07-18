package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.marshall.MarshallerUtil.bytes2obj;
import static org.infinispan.client.hotrod.marshall.MarshallerUtil.obj2bytes;
import static org.infinispan.client.hotrod.marshall.MarshallerUtil.obj2stream;

import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.client.hotrod.configuration.RemoteCacheConfiguration;
import org.infinispan.client.hotrod.impl.MarshallerRegistry;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MediaTypeMarshaller;
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

   private final BufferSizePredictor keySizePredictor = new AdaptiveBufferSizePredictor();
   private final BufferSizePredictor valueSizePredictor = new AdaptiveBufferSizePredictor();

   private final DataFormatImpl server;
   private final DataFormatImpl client;
   private MarshallerRegistry marshallerRegistry;
   private Marshaller defaultMarshaller;

   private final class DataFormatImpl implements MediaTypeMarshaller {

      private final MediaType keyType;
      private final MediaType valueType;
      private final Marshaller keyMarshaller;
      private final Marshaller valueMarshaller;

      private DataFormatImpl(MediaType keyType, MediaType valueType,
                             Marshaller keyMarshaller, Marshaller valueMarshaller) {
         this.keyType = keyType;
         this.valueType = valueType;
         this.keyMarshaller = keyMarshaller;
         this.valueMarshaller = valueMarshaller;
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

      private Marshaller resolveKeyMarshaller() {
         if (keyMarshaller != null) return keyMarshaller;
         if (keyType == null) return defaultMarshaller;

         Marshaller forKeyType = marshallerRegistry.getMarshaller(keyType);
         if (forKeyType != null) return forKeyType;
         log.debugf("No marshaller registered for %s, using no-op marshaller", keyType);

         return IdentityMarshaller.INSTANCE;
      }

      private Marshaller resolveValueMarshaller() {
         if (valueMarshaller != null) return valueMarshaller;
         if (valueType == null) return defaultMarshaller;

         Marshaller forValueType = marshallerRegistry.getMarshaller(valueType);
         if (forValueType != null) return forValueType;
         log.debugf("No marshaller registered for %s, using no-op marshaller", valueType);

         return IdentityMarshaller.INSTANCE;
      }

      @Override
      public byte[] keyToBytes(Object key) {
         Marshaller keyMarshaller = resolveKeyMarshaller();
         return obj2bytes(keyMarshaller, key, keySizePredictor);
      }

      @Override
      public void keyToStream(Object key, OutputStream stream) {
         Marshaller keyMarshaller = resolveKeyMarshaller();
         obj2stream(keyMarshaller, key, stream, keySizePredictor);
      }

      @Override
      public byte[] valueToBytes(Object value) {
         Marshaller valueMarshaller = resolveValueMarshaller();
         return obj2bytes(valueMarshaller, value, valueSizePredictor);
      }

      @Override
      public void valueToStream(Object value, OutputStream stream) {
         Marshaller valueMarshaller = resolveValueMarshaller();
         obj2stream(valueMarshaller, value, stream, valueSizePredictor);
      }

      @Override
      public <T> T bytesToKey(byte[] bytes, ClassAllowList allowList) {
         Marshaller keyMarshaller = resolveKeyMarshaller();
         return bytes2obj(keyMarshaller, bytes, isObjectStorage(), allowList);
      }

      @Override
      public <T> T bytesToKey(InputStream inputStream, ClassAllowList allowList) {
         Marshaller keyMarshaller = resolveKeyMarshaller();
         return bytes2obj(keyMarshaller, inputStream, isObjectStorage(), allowList);
      }

      @Override
      public <T> T bytesToValue(byte[] bytes, ClassAllowList allowList) {
         Marshaller valueMarshaller = resolveValueMarshaller();
         return bytes2obj(valueMarshaller, bytes, isObjectStorage(), allowList);
      }

      @Override
      public <T> T bytesToValue(InputStream inputStream, ClassAllowList allowList) {
         Marshaller valueMarshaller = resolveValueMarshaller();
         return bytes2obj(valueMarshaller, inputStream, isObjectStorage(), allowList);
      }

      public boolean match(DataFormatImpl other) {
         if (other == null) return false;

         MediaType mt = getKeyType();
         return mt != null && other.getKeyType() != null && mt.match(other.getKeyType());
      }

      @Override
      public String toString() {
         return "DataFormatImpl{" +
               "keyType=" + keyType +
               ", valueType=" + valueType +
               ", keyMarshaller=" + keyMarshaller +
               ", valueMarshaller=" + valueMarshaller +
               '}';
      }
   }

   private DataFormat(MediaType cKeyType, MediaType cValueType, Marshaller cKeyMarshaller, Marshaller cValueMarshaller,
                      MediaType sKeyType, MediaType sValueType, Marshaller sKeyMarshaller, Marshaller sValueMarshaller) {
      this.server = new DataFormatImpl(sKeyType, sValueType, sKeyMarshaller, sValueMarshaller);
      this.client = new DataFormatImpl(cKeyType, cValueType, cKeyMarshaller, cValueMarshaller);
   }

   private DataFormat(MediaType cKeyType, MediaType cValueType, Marshaller cKeyMarshaller, Marshaller cValueMarshaller) {
      this.server = null;
      this.client = new DataFormatImpl(cKeyType, cValueType, cKeyMarshaller, cValueMarshaller);
   }

   public DataFormat withoutValueType() {
      DataFormat dataFormat;
      if (server != null) {
         dataFormat = new DataFormat(
               client.keyType, null, client.keyMarshaller, null,
               server.keyType, server.valueType, server.keyMarshaller, server.valueMarshaller);
      } else {
         dataFormat = new DataFormat(client.keyType, null, client.keyMarshaller, null);
      }

      dataFormat.marshallerRegistry = this.marshallerRegistry;
      dataFormat.defaultMarshaller = this.defaultMarshaller;
      return dataFormat;
   }

   public MediaType getKeyType() {
      return client.getKeyType();
   }

   public MediaType getValueType() {
      return client.getValueType();
   }

   public Marshaller getDefaultMarshaller() {
      return defaultMarshaller;
   }

   public void initialize(RemoteCacheManager remoteCacheManager, String cacheName) {
      this.marshallerRegistry = remoteCacheManager.getMarshallerRegistry();
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

   public boolean isObjectStorage() {
      return server != null && server.keyType == MediaType.APPLICATION_OBJECT;
   }

   public byte[] keyToBytes(Object key) {
      return client.keyToBytes(key);
   }

   public void keyToStream(Object key, OutputStream stream) {
      client.keyToStream(key, stream);
   }

   public byte[] valueToBytes(Object value) {
      return client.valueToBytes(value);
   }

   public void valueToStream(Object value, OutputStream stream) {
      client.valueToStream(value, stream);
   }

   public <T> T keyToObj(byte[] bytes, ClassAllowList allowList) {
      return client.bytesToKey(bytes, allowList);
   }

   public <T> T keyToObject(InputStream inputStream, ClassAllowList allowList) {
      return client.bytesToKey(inputStream, allowList);
   }

   public <T> T valueToObj(byte[] bytes, ClassAllowList allowList) {
      return client.bytesToValue(bytes, allowList);
   }

   public <T> T valueToObj(InputStream inputStream, ClassAllowList allowList) {
      return client.bytesToValue(inputStream, allowList);
   }

   public MediaTypeMarshaller server() {
      if (server == null || server.match(client)) return null;

      // If is an object storage, we hash the actual object to retrieve the segment.
      // We return null so the client does not use anything from the server.
      if (isObjectStorage()) return null;

      // When the registry returns `null` means that the client DOES NOT have a marshaller capable of converting to
      // the server key type. Therefore, we return null, so it will utilize the default and NOT convert the object.
      // This could cause additional redirections on the server side and poor performance for the client.
      return marshallerRegistry.getMarshaller(server.keyType) == null ? null : server;
   }

   public MediaTypeMarshaller client() {
      return client;
   }

   @Override
   public String toString() {
      return "DataFormat{" +
            "client=" + client +
            ", server=" + server +
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
      private Builder serverDataFormat;

      public Builder from(DataFormat dataFormat) {
         from(dataFormat.client);
         if (dataFormat.server != null) {
            this.serverDataFormat = new Builder();
            this.serverDataFormat.from(dataFormat.server);
         }
         return this;
      }

      private void from(DataFormatImpl impl) {
         this.keyType = impl.keyType;
         this.valueType = impl.valueType;
         this.keyMarshaller = impl.keyMarshaller;
         this.valueMarshaller = impl.valueMarshaller;
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

      public Builder serverDataFormat(Builder format) {
         this.serverDataFormat = format;
         return this;
      }

      public DataFormat build() {
         if (serverDataFormat != null) {
            return new DataFormat(
                  keyType, valueType, keyMarshaller, valueMarshaller,
                  serverDataFormat.keyType, serverDataFormat.valueType, serverDataFormat.keyMarshaller, serverDataFormat.valueMarshaller
            );
         }

         return new DataFormat(keyType, valueType, keyMarshaller, valueMarshaller);
      }

   }
}
