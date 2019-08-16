package org.infinispan.api.client.impl;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.api.Infinispan;
import org.infinispan.api.client.configuration.InfinispanClientConfigImpl;
import org.infinispan.api.configuration.ClientConfig;
import org.infinispan.api.exception.InfinispanConfigurationException;
import org.infinispan.api.exception.InfinispanException;
import org.infinispan.api.marshalling.Marshaller;
import org.infinispan.api.reactive.KeyValueStore;
import org.infinispan.api.reactive.KeyValueStoreConfig;
import org.infinispan.api.reactive.client.impl.KeyValueStoreImpl;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.annotations.ProtoSchemaBuilder;

public class InfinispanClientImpl implements Infinispan {

   private RemoteCacheManager cacheManager;
   private Set<String> files = ConcurrentHashMap.newKeySet();
   private ExecutorService asyncExecutorService;

   public InfinispanClientImpl(ClientConfig infinispanClientConfig) {
      if (infinispanClientConfig instanceof InfinispanClientConfigImpl) {
         InfinispanClientConfigImpl infinispanClientConfigImpl = (InfinispanClientConfigImpl) infinispanClientConfig;
         cacheManager = new RemoteCacheManager(infinispanClientConfigImpl.getConfiguration());
      } else {
         throw new InfinispanConfigurationException("Unable to construct InfinispanClientImpl. ClientConfig is not a InfinispanClientConfigImpl");
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
         SerializationContext ctx = MarshallerUtil.getSerializationContext(cacheManager);
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
      SerializationContext ctx = MarshallerUtil.getSerializationContext(cacheManager);

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
