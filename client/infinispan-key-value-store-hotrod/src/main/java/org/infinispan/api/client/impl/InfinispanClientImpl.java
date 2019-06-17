package org.infinispan.api.client.impl;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.ClientConfig;
import org.infinispan.api.Infinispan;
import org.infinispan.api.collections.reactive.client.impl.KeyValueStoreImpl;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.api.marshalling.Marshaller;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;

public class InfinispanClientImpl implements Infinispan {

   private RemoteCacheManager cacheManager;

   public InfinispanClientImpl(ClientConfig clientConfig) {
      if (clientConfig instanceof ClientConfigurationLoader.ConfigurationWrapper) {
         ClientConfigurationLoader.ConfigurationWrapper wrapper = (ClientConfigurationLoader.ConfigurationWrapper) clientConfig;
         cacheManager = new RemoteCacheManager(wrapper.getConfiguration());
      }
   }

   /**
    * Visible for testing
    *
    * @param remoteCacheManager
    */
   public InfinispanClientImpl(RemoteCacheManager remoteCacheManager) {
      cacheManager = remoteCacheManager;
   }

   @Override
   public <K, V> KeyValueStore<K, V> getKeyValueStore(String name) {
      RemoteCache<K, V> cache = cacheManager.getCache(name, false);
      RemoteCache<K, V> cacheWithReturnValues = cacheManager.getCache(name, true);
      return new KeyValueStoreImpl(cache, cacheWithReturnValues);
   }

   @Override
   public <K, V> KeyValueStore<K, V> getKeyValueStore(String name, KeyValueStoreConfig config) {
      addProtobufSchema(config);
      return getKeyValueStore(name);
   }

   @Override
   public CompletionStage<Void> stop() {
      return cacheManager.stopAsync();
   }

   private void addProtobufSchema(KeyValueStoreConfig config) {
      String fileName = config.getSchemaFileName();
      // Retrieve metadata cache
      RemoteCache<String, String> metadataCache =
            cacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);

      SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(cacheManager);

      // TODO: Does not work https://issues.jboss.org/browse/ISPN-9973
      for (Marshaller marshaller : config.getMarshallers()) {
         ctx.registerMarshaller((BaseMarshaller) marshaller);
      }

      // Use ProtoSchemaBuilder to define
      ProtoSchemaBuilder protoSchemaBuilder = new ProtoSchemaBuilder();
      String protoFile;
      try {
         protoFile = protoSchemaBuilder
               .fileName(fileName)
               .addClass(config.getValueClazz())
               .packageName(config.getPackageName())
               .build(ctx);
      } catch (IOException e) {
         // TODO handle error
         throw new RuntimeException(e);
      }
      // Store the configuration in the metadata cache
      metadataCache.put(fileName, protoFile);
   }
}
