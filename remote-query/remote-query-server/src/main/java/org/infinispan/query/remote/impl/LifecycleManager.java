package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.util.Map;

import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.impl.Externalizers.QueryRequestExternalizer;
import org.infinispan.query.remote.client.impl.MarshallerRegistration;
import org.infinispan.query.remote.client.impl.QueryRequest;
import org.infinispan.query.remote.impl.filter.ContinuousQueryResultExternalizer;
import org.infinispan.query.remote.impl.filter.FilterResultExternalizer;
import org.infinispan.query.remote.impl.filter.IckleBinaryProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.filter.IckleContinuousQueryProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.impl.filter.IckleProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.impl.filter.IckleProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapperSearchWorkCreator;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.query.remote.impl.persistence.PersistenceContextInitializerImpl;
import org.infinispan.registry.InternalCacheRegistry;

/**
 * Initializes components for remote query. Each cache manager has its own instance of this class during its lifetime.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@InfinispanModule(name = "remote-query-server", requiredModules = {"core", "query", "server-core"})
public final class LifecycleManager implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.ICKLE_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER, new IckleProtobufCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_PROTOBUF_FILTER_AND_CONVERTER, new IckleProtobufFilterAndConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER, new IckleContinuousQueryProtobufCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_BINARY_PROTOBUF_FILTER_AND_CONVERTER, new IckleBinaryProtobufFilterAndConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CONTINUOUS_QUERY_RESULT, new ContinuousQueryResultExternalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_FILTER_RESULT, new FilterResultExternalizer());

      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);
      SerializationContextRegistry ctxRegistry = gcr.getComponent(SerializationContextRegistry.class);
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.PERSISTENCE, new PersistenceContextInitializerImpl());
      ctxRegistry.addContextInitializer(SerializationContextRegistry.MarshallerType.GLOBAL, MarshallerRegistration.INSTANCE);

      initProtobufMetadataManager(bcr);

      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      cacheManager.getClassWhiteList()
            .addClasses(QueryRequest.class, QueryRequestExternalizer.class);
   }

   private void initProtobufMetadataManager(BasicComponentRegistry bcr) {
      ProtobufMetadataManagerImpl protobufMetadataManager = new ProtobufMetadataManagerImpl();
      bcr.registerComponent(ProtobufMetadataManager.class, protobufMetadataManager, true).running();

      EncoderRegistry encoderRegistry = bcr.getComponent(EncoderRegistry.class).wired();
      encoderRegistry.registerWrapper(ProtobufWrapper.INSTANCE);
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      BasicComponentRegistry bcr = gcr.getComponent(BasicComponentRegistry.class);

      ProtobufMetadataManagerImpl protobufMetadataManager =
            (ProtobufMetadataManagerImpl) bcr.getComponent(ProtobufMetadataManager.class).running();

      // if not already running, start it
      protobufMetadataManager.getCache();

      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      if (globalCfg.statistics()) {
         registerProtobufMetadataManagerMBean(protobufMetadataManager, globalCfg, bcr);
      }
   }

   private void registerProtobufMetadataManagerMBean(ProtobufMetadataManagerImpl protobufMetadataManager,
                                                     GlobalConfiguration globalConfig, BasicComponentRegistry bcr) {
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

   /**
    * Registers the interceptor in the cache before it gets started.
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      BasicComponentRegistry gcr = cr.getGlobalComponentRegistry().getComponent(BasicComponentRegistry.class);

      if (PROTOBUF_METADATA_CACHE_NAME.equals(cacheName)) {
         // a protobuf metadata cache is starting, need to register the interceptor
         ProtobufMetadataManagerImpl protobufMetadataManager =
               (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class).running();
         protobufMetadataManager.addProtobufMetadataManagerInterceptor(cr.getComponent(BasicComponentRegistry.class));
      }

      InternalCacheRegistry icr = gcr.getComponent(InternalCacheRegistry.class).running();
      if (!icr.isInternalCache(cacheName)) {
         // a stop dependency must be added for each non-internal cache
         ProtobufMetadataManagerImpl protobufMetadataManager =
            (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class).running();
         protobufMetadataManager.addCacheDependency(cacheName);

         // a remote query manager must be added for each non-internal cache
         SerializationContext serCtx = protobufMetadataManager.getSerializationContext();
         RemoteQueryManager remoteQueryManager = buildQueryManager(cfg, serCtx, cr);
         cr.registerComponent(remoteQueryManager, RemoteQueryManager.class);
      }
   }

   private RemoteQueryManager buildQueryManager(Configuration cfg, SerializationContext ctx, ComponentRegistry cr) {
      ContentTypeConfiguration valueEncoding = cfg.encoding().valueDataType();
      MediaType valueStorageMediaType = valueEncoding.mediaType();
      MediaType storageMediaType = cr.getComponent(Cache.class).getAdvancedCache().getValueDataConversion().getStorageMediaType();
      QuerySerializers querySerializers = buildQuerySerializers(cr, storageMediaType);

      boolean isObjectStorage = valueStorageMediaType != null && valueStorageMediaType.match(APPLICATION_OBJECT);
      if (isObjectStorage) return new ObjectRemoteQueryManager(cr, querySerializers);
      return new ProtobufRemoteQueryManager(ctx, cr, querySerializers);
   }

   private QuerySerializers buildQuerySerializers(ComponentRegistry cr, MediaType storageMediaType) {
      EncoderRegistry encoderRegistry = cr.getGlobalComponentRegistry().getComponent(EncoderRegistry.class);
      QuerySerializers querySerializers = new QuerySerializers();
      DefaultQuerySerializer defaultQuerySerializer = new DefaultQuerySerializer(encoderRegistry);
      querySerializers.addSerializer(MediaType.MATCH_ALL, defaultQuerySerializer);

      if (encoderRegistry.isConversionSupported(storageMediaType, APPLICATION_JSON)) {
         Transcoder jsonStorage = encoderRegistry.getTranscoder(APPLICATION_JSON, storageMediaType);
         Transcoder jsonObject = encoderRegistry.getTranscoder(APPLICATION_JSON, APPLICATION_OBJECT);
         querySerializers.addSerializer(APPLICATION_JSON, new JsonQuerySerializer(storageMediaType, jsonStorage, jsonObject));
      }
      return querySerializers;
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      GlobalComponentRegistry gcr = cr.getGlobalComponentRegistry();
      InternalCacheRegistry icr = gcr.getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         Configuration cfg = cr.getComponent(Configuration.class);
         if (cfg.indexing().index().isEnabled()) {
            ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class);
            SerializationContext serCtx = protobufMetadataManager.getSerializationContext();

            log.debugf("Wrapping the SearchWorkCreator for indexed cache %s", cacheName);
            QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
            queryInterceptor.setSearchWorkCreator(new ProtobufValueWrapperSearchWorkCreator(queryInterceptor.getSearchWorkCreator(), serCtx).get());
         }
      }
   }
}
