package org.infinispan.server.core;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.internal.InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME;

import javax.management.ObjectName;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.Indexer;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.impl.EntityLoaderFactory;
import org.infinispan.query.impl.IndexStartupRunner;
import org.infinispan.query.impl.massindex.DistributedExecutorMassIndexer;
import org.infinispan.query.mapper.mapping.SearchMapping;
import org.infinispan.query.mapper.mapping.SearchMappingCommonBuilding;
import org.infinispan.query.stats.impl.LocalIndexStatistics;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.core.dataconversion.JsonTranscoder;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.server.core.query.impl.DefaultQuerySerializer;
import org.infinispan.server.core.query.impl.JsonQuerySerializer;
import org.infinispan.server.core.query.impl.LazySearchMapping;
import org.infinispan.server.core.query.impl.ObjectRemoteQueryManager;
import org.infinispan.server.core.query.impl.ProtobufMetadataManagerImpl;
import org.infinispan.server.core.query.impl.ProtobufRemoteQueryManager;
import org.infinispan.server.core.query.impl.QuerySerializers;
import org.infinispan.server.core.query.impl.RemoteQueryAccessEngine;
import org.infinispan.server.core.query.impl.RemoteQueryManager;
import org.infinispan.tasks.query.RemoteQueryAccess;

/**
 * Server module lifecycle callbacks
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@InfinispanModule(name = "server-core", requiredModules = {"core", "query"}, optionalModules = {"jboss-marshalling", "json-pojo-transcoder", "xml-transcoder"})
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerTranscoder(new JsonTranscoder());

      // Query
      org.infinispan.query.impl.LifecycleManager queryModule = gcr.getModuleLifecycle(org.infinispan.query.impl.LifecycleManager.class);
      if (queryModule != null) {
         queryModule.enableRemoteQuery();
      }

      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new org.infinispan.server.core.query.impl.persistence.PersistenceContextInitializerImpl());
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, org.infinispan.query.remote.client.impl.GlobalContextInitializerImpl.INSTANCE);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, org.infinispan.server.core.query.impl.GlobalContextInitializerImpl.INSTANCE);

      initProtobufMetadataManager(bcr);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);

      ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) bcr.getComponent(ProtobufMetadataManager.class).running();

      // if not already running, start it
      protobufMetadataManager.getCache();

      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      if (globalCfg.jmx().enabled()) {
         registerProtobufMetadataManagerMBean(protobufMetadataManager, globalCfg, bcr);
      }
   }

   /**
    * Registers the interceptor in the {@link InternalCacheNames#PROTOBUF_METADATA_CACHE_NAME} cache before it gets started. Also creates query components
    * for user caches.
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      BasicComponentRegistry gcr = cr.getGlobalComponentRegistry().getComponent(BasicComponentRegistry.class);
      LocalQueryStatistics queryStatistics = cr.getComponent(LocalQueryStatistics.class);
      if (PROTOBUF_METADATA_CACHE_NAME.equals(cacheName)) {
         // a protobuf metadata cache is starting, need to register the interceptor
         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class).running();
         protobufMetadataManager.addProtobufMetadataManagerInterceptor(cr.getComponent(BasicComponentRegistry.class));
      }

      InternalCacheRegistry icr = gcr.getComponent(InternalCacheRegistry.class).running();
      if (!icr.isInternalCache(cacheName)) {
         // a stop dependency must be added for each non-internal cache
         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class).running();
         protobufMetadataManager.addCacheDependency(cacheName);

         // a remote query manager must be added for each non-internal cache
         SerializationContext serCtx = protobufMetadataManager.getSerializationContext();

         SearchMappingCommonBuilding commonBuilding = cr.getComponent(SearchMappingCommonBuilding.class);
         SearchMapping searchMapping = cr.getComponent(SearchMapping.class);
         if (commonBuilding != null && searchMapping == null) {
            AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache().withStorageMediaType();

            EntityLoaderFactory<?> entityLoader = new EntityLoaderFactory<>(cache, queryStatistics);

            QueryCache queryCache = cr.getGlobalComponentRegistry().getComponent(QueryCache.class);
            searchMapping = new LazySearchMapping(commonBuilding, entityLoader, serCtx, cache, protobufMetadataManager, queryCache);

            cr.registerComponent(searchMapping, SearchMapping.class);
            Indexer indexer = cr.getComponent(Indexer.class);
            if (indexer == null) {
               Indexer massIndexer = new DistributedExecutorMassIndexer(cache);
               cr.registerComponent(massIndexer, Indexer.class);
            }
            BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
            bcr.replaceComponent(IndexStatistics.class.getName(), new LocalIndexStatistics(), true);
            bcr.rewire();
            new IndexStartupRunner(cache).run();
         }

         RemoteQueryManager remoteQueryManager = buildQueryManager(cfg, serCtx, cr, searchMapping);
         cr.registerComponent(remoteQueryManager, RemoteQueryManager.class);
         cr.registerComponent(new RemoteQueryAccessEngine(), RemoteQueryAccess.class);
      }
   }

   private void initProtobufMetadataManager(BasicComponentRegistry bcr) {
      ProtobufMetadataManagerImpl protobufMetadataManager = new ProtobufMetadataManagerImpl();
      bcr.registerComponent(ProtobufMetadataManager.class, protobufMetadataManager, true).running();
   }

   private void registerProtobufMetadataManagerMBean(ProtobufMetadataManagerImpl protobufMetadataManager, GlobalConfiguration globalConfig, BasicComponentRegistry bcr) {
      CacheManagerJmxRegistration jmxRegistration = bcr.getComponent(CacheManagerJmxRegistration.class).running();
      try {
         jmxRegistration.registerMBean(protobufMetadataManager, getRemoteQueryGroupName(globalConfig));
      } catch (Exception e) {
         throw new CacheException("Unable to register ProtobufMetadataManager MBean", e);
      }
   }

   private String getRemoteQueryGroupName(GlobalConfiguration globalConfig) {
      return "type=RemoteQuery,name=" + ObjectName.quote(globalConfig.cacheManagerName());
   }

   private RemoteQueryManager buildQueryManager(Configuration cfg, SerializationContext ctx, ComponentRegistry cr, SearchMapping searchMapping) {
      ContentTypeConfiguration valueEncoding = cfg.encoding().valueDataType();
      MediaType valueStorageMediaType = valueEncoding.mediaType();
      AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
      MediaType storageMediaType = cache.getValueDataConversion().getStorageMediaType();
      QuerySerializers querySerializers = buildQuerySerializers(cr, storageMediaType);

      boolean isObjectStorage = valueStorageMediaType != null && valueStorageMediaType.match(APPLICATION_OBJECT);
      if (isObjectStorage) return new ObjectRemoteQueryManager(cache, cr, querySerializers);
      return new ProtobufRemoteQueryManager(cache, cr, ctx, querySerializers, searchMapping);
   }

   private QuerySerializers buildQuerySerializers(ComponentRegistry cr, MediaType storageMediaType) {
      EncoderRegistry encoderRegistry = cr.getGlobalComponentRegistry().getComponent(EncoderRegistry.class);
      QuerySerializers querySerializers = new QuerySerializers();
      DefaultQuerySerializer defaultQuerySerializer = new DefaultQuerySerializer(encoderRegistry);
      querySerializers.addSerializer(MediaType.MATCH_ALL, defaultQuerySerializer);

      if (encoderRegistry.isConversionSupported(storageMediaType, APPLICATION_JSON)) {
         Transcoder jsonStorage = encoderRegistry.getTranscoder(APPLICATION_JSON, storageMediaType);
         querySerializers.addSerializer(APPLICATION_JSON, new JsonQuerySerializer(storageMediaType, jsonStorage));
      }
      return querySerializers;
   }
}
