package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.query.remote.ProtobufMetadataManager.SCHEMA_MANAGER_ROLE;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtostreamSerializationContextInitializer;
import org.infinispan.query.remote.client.impl.Externalizers.QueryRequestExternalizer;
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
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.CacheRoleImpl;
import org.infinispan.server.core.dataconversion.ProtostreamJsonTranscoder;
import org.infinispan.server.core.dataconversion.ProtostreamObjectTranscoder;
import org.infinispan.server.core.dataconversion.ProtostreamTextTranscoder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Initializes components for remote query. Each cache manager has its own instance of this class during its lifetime.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@InfinispanModule(name = "remote-query-server", requiredModules = "core")
public final class LifecycleManager implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   /**
    * Caching the looked-up MBeanServer for the lifetime of the cache manager is safe.
    */
   private MBeanServer mbeanServer;

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
      PersistenceMarshaller persistenceMarshaller = bcr.getComponent(KnownComponentNames.PERSISTENCE_MARSHALLER, PersistenceMarshaller.class).wired();
      persistenceMarshaller.register(new PersistenceContextInitializerImpl());

      InternalCacheRegistry icr = bcr.getComponent(InternalCacheRegistry.class).running();
      registerProtobufMetadataCache(icr, globalCfg);

      initProtobufMetadataManager(globalCfg, gcr, bcr);

      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      cacheManager.getClassWhiteList()
            .addClasses(QueryRequest.class, QueryRequestExternalizer.class);
   }

   private void initProtobufMetadataManager(GlobalConfiguration globalCfg, GlobalComponentRegistry gcr, BasicComponentRegistry bcr) {
      ProtobufMetadataManagerImpl protobufMetadataManager = new ProtobufMetadataManagerImpl();
      bcr.registerComponent(ProtobufMetadataManager.class, protobufMetadataManager, true).running();
      if (globalCfg.globalJmxStatistics().enabled()) {
         registerProtobufMetadataManagerMBean(protobufMetadataManager, gcr);
      }

      SerializationContext serCtx = protobufMetadataManager.getSerializationContext();
      ClassLoader classLoader = globalCfg.classLoader();
      processProtostreamSerializationContextInitializers(classLoader, serCtx);

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerWrapper(ProtobufWrapper.INSTANCE);
      encoderRegistry.registerTranscoder(new ProtostreamJsonTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamTextTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamObjectTranscoder(serCtx, classLoader));
   }

   private void processProtostreamSerializationContextInitializers(ClassLoader classLoader, SerializationContext serCtx) {
      Collection<ProtostreamSerializationContextInitializer> initializers =
            ServiceFinder.load(ProtostreamSerializationContextInitializer.class, classLoader);

      for (ProtostreamSerializationContextInitializer psci : initializers) {
         try {
            psci.init(serCtx);
         } catch (Exception e) {
            throw log.errorInitializingSerCtx(e);
         }
      }
   }

   private void registerProtobufMetadataManagerMBean(ProtobufMetadataManagerImpl protobufMetadataManager, GlobalComponentRegistry gcr) {
      GlobalJmxStatisticsConfiguration jmxConfig = gcr.getGlobalConfiguration().globalJmxStatistics();
      if (mbeanServer == null) {
         mbeanServer = JmxUtil.lookupMBeanServer(jmxConfig.mbeanServerLookup(), jmxConfig.properties());
      }

      String groupName = "type=RemoteQuery,name=" + ObjectName.quote(jmxConfig.cacheManagerName());
      String jmxDomain = JmxUtil.buildJmxDomain(jmxConfig.domain(), mbeanServer, groupName);
      BasicComponentRegistry basicComponentRegistry = gcr.getComponent(BasicComponentRegistry.class);
      MBeanMetadata metadata = basicComponentRegistry.getMBeanMetadata(ProtobufMetadataManagerImpl.class.getName());

      try {
         ResourceDMBean mBean = new ResourceDMBean(protobufMetadataManager, metadata, null);
         ObjectName objName = new ObjectName(jmxDomain + ":" + groupName + ",component=" + metadata.getJmxObjectName());
         protobufMetadataManager.setObjectName(objName);
         JmxUtil.registerMBean(mBean, objName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to register ProtobufMetadataManager MBean", e);
      }
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      if (gcr.getGlobalConfiguration().globalJmxStatistics().enabled()) {
         unregisterProtobufMetadataManagerMBean(gcr);
      }
   }

   private void unregisterProtobufMetadataManagerMBean(GlobalComponentRegistry gcr) {
      if (mbeanServer != null) {
         try {
            ProtobufMetadataManager protobufMetadataManager = gcr.getComponent(ProtobufMetadataManager.class);
            if (protobufMetadataManager != null) {
               JmxUtil.unregisterMBean(protobufMetadataManager.getObjectName(), mbeanServer);
            }
         } catch (Exception e) {
            throw new CacheException("Unable to unregister ProtobufMetadataManager MBean", e);
         }
      }
   }

   /**
    * Registers the remote value wrapper interceptor in the cache before it gets started.
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      BasicComponentRegistry gcr = cr.getGlobalComponentRegistry().getComponent(BasicComponentRegistry.class);

      if (PROTOBUF_METADATA_CACHE_NAME.equals(cacheName)) {
         BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
         ProtobufMetadataManagerInterceptor protobufInterceptor = new ProtobufMetadataManagerInterceptor();
         bcr.registerComponent(ProtobufMetadataManagerInterceptor.class, protobufInterceptor, true);
         bcr.addDynamicDependency(AsyncInterceptorChain.class.getName(), ProtobufMetadataManagerInterceptor.class.getName());
         bcr.getComponent(AsyncInterceptorChain.class).wired()
            .addInterceptorAfter(protobufInterceptor, EntryWrappingInterceptor.class);
      }

      InternalCacheRegistry icr = gcr.getComponent(InternalCacheRegistry.class).running();
      if (!icr.isInternalCache(cacheName)) {
         ProtobufMetadataManagerImpl protobufMetadataManager =
            (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class).running();
         protobufMetadataManager.addCacheDependency(cacheName);

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
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         Configuration cfg = cr.getComponent(Configuration.class);
         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         SerializationContext serCtx = protobufMetadataManager.getSerializationContext();

         if (cfg.indexing().index().isEnabled()) {
            log.debugf("Wrapping the SearchWorkCreator for indexed cache %s", cacheName);
            QueryInterceptor queryInterceptor = cr.getComponent(QueryInterceptor.class);
            queryInterceptor.setSearchWorkCreator(new ProtobufValueWrapperSearchWorkCreator(queryInterceptor.getSearchWorkCreator(), serCtx).get());
         }
      }
   }

   @Override
   public void cacheManagerStopped(GlobalComponentRegistry gcr) {
      mbeanServer = null;
   }


   private void registerProtobufMetadataCache(InternalCacheRegistry internalCacheRegistry,
                                              GlobalConfiguration globalConfiguration) {
      internalCacheRegistry.registerInternalCache(PROTOBUF_METADATA_CACHE_NAME,
                                                  getProtobufMetadataCacheConfig(globalConfiguration).build(),
                                                  EnumSet.of(InternalCacheRegistry.Flag.USER,
                                                             InternalCacheRegistry.Flag.PROTECTED,
                                                             InternalCacheRegistry.Flag.PERSISTENT));
   }

   private ConfigurationBuilder getProtobufMetadataCacheConfig(GlobalConfiguration globalConfiguration) {
      CacheMode cacheMode = globalConfiguration.isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;

      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.transaction()
         .transactionMode(TransactionMode.TRANSACTIONAL).invocationBatching().enable()
         .transaction().lockingMode(LockingMode.PESSIMISTIC)
         .locking().isolationLevel(IsolationLevel.READ_COMMITTED).useLockStriping(false)
         .clustering().cacheMode(cacheMode)
         .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false)
         .encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
         .encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      if (globalConfiguration.security().authorization().enabled()) {
         globalConfiguration.security().authorization().roles()
                            .put(SCHEMA_MANAGER_ROLE,
                                 new CacheRoleImpl(SCHEMA_MANAGER_ROLE, AuthorizationPermission.ALL));
         cfg.security().authorization().enable().role(SCHEMA_MANAGER_ROLE);
      }
      return cfg;
   }
}
