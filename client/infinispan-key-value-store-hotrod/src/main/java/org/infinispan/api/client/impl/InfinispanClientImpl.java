package org.infinispan.api.client.impl;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.api.configuration.ClientConfig;
import org.infinispan.api.Infinispan;
import org.infinispan.api.exception.InfinispanConfigurationException;
import org.infinispan.api.store.client.impl.KeyValueStoreImpl;
import org.infinispan.api.exception.InfinispanException;
import org.infinispan.api.marshalling.Marshaller;
import org.infinispan.api.store.KeyValueStore;
import org.infinispan.api.store.KeyValueStoreConfig;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;

public class InfinispanClientImpl implements Infinispan {

   private RemoteCacheManager cacheManager;
   private Set<String> files = ConcurrentHashMap.newKeySet();
   private ExecutorService asyncExecutorService;

   public InfinispanClientImpl(ClientConfig clientConfig) {
      if (clientConfig instanceof ClientConfigurationLoader.ConfigurationWrapper) {
         ClientConfigurationLoader.ConfigurationWrapper wrapper = (ClientConfigurationLoader.ConfigurationWrapper) clientConfig;
         cacheManager = new RemoteCacheManager(wrapper.getConfiguration());
      } else {
         //TODO Handle this better in ISPN-9929
         throw new InfinispanConfigurationException("Unable to construct InfinispanClientImpl. ClientConfig is not a ClientConfigurationLoader.ConfigurationWrapper");
      }
      asyncExecutorService = cacheManager.getAsyncExecutorService();
   }

   /**
    * Visible for testing
    *
    * @param remoteCacheManager
    */
   public InfinispanClientImpl(RemoteCacheManager remoteCacheManager) {
      cacheManager = remoteCacheManager;
      asyncExecutorService = cacheManager.getAsyncExecutorService();
   }

   @Override
   public <K, V> CompletionStage<KeyValueStore<K, V>> getKeyValueStore(String name, KeyValueStoreConfig config) {
      return CompletableFuture.supplyAsync(() -> {
         addProtobufSchema(config);
         RemoteCache<K, V> cache = cacheManager.getCache(name, false);
         RemoteCache<K, V> cacheWithReturnValues = cacheManager.getCache(name, true);
         return new KeyValueStoreImpl<>(cache, cacheWithReturnValues);
      }, asyncExecutorService);
   }

   @Override
   public CompletionStage<Void> stop() {
      if (!files.isEmpty()) {
         SerializationContext ctx = ProtoStreamMarshaller.getSerializationContext(cacheManager);
         files.forEach(ctx::unregisterProtoFile);
      }
      files.clear();
      return cacheManager.stopAsync();
   }

   synchronized private void addProtobufSchema(KeyValueStoreConfig config) {
      String fileName = config.getSchemaFileName();
      if (fileName == null) return;

      // Retrieve metadata cache
      RemoteCache<String, String> metadataCache =
            cacheManager.getCache(PROTOBUF_METADATA_CACHE_NAME);

      String file = metadataCache.get(fileName);

      if (file != null) return;
      files.add(fileName);
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
         throw new InfinispanException("Error encountered when adding proto file", e);
      }
      // Store the configuration in the metadata cache
      metadataCache.putIfAbsentAsync(fileName, protoFile);
   }
}
