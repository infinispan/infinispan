package org.infinispan.query.remote.impl;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.interceptors.BatchingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.ProtobufMatcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.impl.filter.ContinuousQueryResultExternalizer;
import org.infinispan.query.remote.impl.filter.JPABinaryProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.filter.JPAContinuousQueryProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.impl.filter.JPAProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.impl.filter.JPAProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.impl.indexing.RemoteValueWrapperInterceptor;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.kohsuke.MetaInfServices;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public final class LifecycleManager extends AbstractModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.PROTOBUF_VALUE_WRAPPER, new ProtobufValueWrapper.Externalizer());
      externalizerMap.put(ExternalizerIds.JPA_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER, new JPAProtobufCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.JPA_PROTOBUF_FILTER_AND_CONVERTER, new JPAProtobufFilterAndConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.JPA_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER, new JPAContinuousQueryProtobufCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.JPA_BINARY_PROTOBUF_FILTER_AND_CONVERTER, new JPABinaryProtobufFilterAndConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.JPA_CONTINUOUS_QUERY_RESULT, new ContinuousQueryResultExternalizer());
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      initProtobufMetadataManager((DefaultCacheManager) cacheManager, gcr);
   }

   private void initProtobufMetadataManager(DefaultCacheManager cacheManager, GlobalComponentRegistry gcr) {
      ProtobufMetadataManagerImpl protobufMetadataManager = new ProtobufMetadataManagerImpl();
      gcr.registerComponent(protobufMetadataManager, ProtobufMetadataManager.class);
      registerProtobufMetadataManagerMBean(protobufMetadataManager, gcr, cacheManager.getName());
   }

   private void registerProtobufMetadataManagerMBean(ProtobufMetadataManager protobufMetadataManager, GlobalComponentRegistry gcr, String cacheManagerName) {
      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      MBeanServer mBeanServer = JmxUtil.lookupMBeanServer(globalCfg);

      String groupName = "type=RemoteQuery,name=" + ObjectName.quote(cacheManagerName);
      String jmxDomain = JmxUtil.buildJmxDomain(globalCfg, mBeanServer, groupName);
      ComponentMetadataRepo metadataRepo = gcr.getComponentMetadataRepo();
      ManageableComponentMetadata metadata = metadataRepo.findComponentMetadata(ProtobufMetadataManagerImpl.class)
            .toManageableComponentMetadata();
      try {
         ResourceDMBean mBean = new ResourceDMBean(protobufMetadataManager, metadata);
         ObjectName objName = new ObjectName(jmxDomain + ":" + groupName + ",component=" + metadata.getJmxObjectName());
         protobufMetadataManager.setObjectName(objName);
         JmxUtil.registerMBean(mBean, objName, mBeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to register ProtobufMetadataManager MBean", e);
      }
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      unregisterProtobufMetadataManagerMBean(gcr);
   }

   private void unregisterProtobufMetadataManagerMBean(GlobalComponentRegistry gcr) {
      try {
         ObjectName objName = gcr.getComponent(ProtobufMetadataManager.class).getObjectName();
         MBeanServer mBeanServer = JmxUtil.lookupMBeanServer(gcr.getGlobalConfiguration());
         JmxUtil.unregisterMBean(objName, mBeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to unregister ProtobufMetadataManager MBean", e);
      }
   }

   /**
    * Registers the remote value wrapper interceptor in the cache before it gets started.
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         boolean isIndexed = cfg.indexing().index().isEnabled();
         boolean isCompatMode = cfg.compatibility().enabled();
         if (isIndexed && !isCompatMode) {
            log.infof("Registering RemoteValueWrapperInterceptor for cache %s", cacheName);
            createRemoteValueWrapperInterceptor(cr, cfg);
         }
      }
   }

   private void createRemoteValueWrapperInterceptor(ComponentRegistry cr, Configuration cfg) {
      RemoteValueWrapperInterceptor wrapperInterceptor = cr.getComponent(RemoteValueWrapperInterceptor.class);
      if (wrapperInterceptor == null) {
         wrapperInterceptor = new RemoteValueWrapperInterceptor();

         // Interceptor registration not needed, core configuration handling
         // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
         InterceptorChain ic = cr.getComponent(InterceptorChain.class);

         ConfigurationBuilder builder = new ConfigurationBuilder().read(cfg);
         InterceptorConfigurationBuilder interceptorBuilder = builder.customInterceptors().addInterceptor();
         interceptorBuilder.interceptor(wrapperInterceptor);

         if (cfg.invocationBatching().enabled()) {
            if (ic != null) ic.addInterceptorAfter(wrapperInterceptor, BatchingInterceptor.class);
            interceptorBuilder.after(BatchingInterceptor.class);
         } else {
            if (ic != null) ic.addInterceptorAfter(wrapperInterceptor, InvocationContextInterceptor.class);
            interceptorBuilder.after(InvocationContextInterceptor.class);
         }
         if (ic != null) {
            cr.registerComponent(wrapperInterceptor, RemoteValueWrapperInterceptor.class);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         Configuration cfg = cr.getComponent(Configuration.class);
         boolean isIndexed = cfg.indexing().index().isEnabled();
         boolean isCompatMode = cfg.compatibility().enabled();
         if (isIndexed && !isCompatMode) {
            if (!verifyChainContainsRemoteValueWrapperInterceptor(cr)) {
               throw new IllegalStateException("It was expected to find the RemoteValueWrapperInterceptor registered in the InterceptorChain but it wasn't found");
            }
         } else if (verifyChainContainsRemoteValueWrapperInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the RemoteValueWrapperInterceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }

         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         SerializationContext serCtx = protobufMetadataManager.getSerializationContext();
         cr.registerComponent(new ProtobufMatcher(serCtx), ProtobufMatcher.class);

         if (isCompatMode) {
            SearchIntegrator searchFactory = cr.getComponent(SearchIntegrator.class);
            CompatibilityReflectionMatcher compatibilityReflectionMatcher;
            if (searchFactory == null) {
               compatibilityReflectionMatcher = new CompatibilityReflectionMatcher(serCtx);
            } else {
               compatibilityReflectionMatcher = new CompatibilityReflectionMatcher(serCtx, searchFactory);
            }
            cr.registerComponent(compatibilityReflectionMatcher, CompatibilityReflectionMatcher.class);
         }

         AdvancedCache<?, ?> cache = cr.getComponent(Cache.class).getAdvancedCache();
         RemoteQueryEngine remoteQueryEngine = new RemoteQueryEngine(cache, isIndexed, isCompatMode, serCtx);
         cr.registerComponent(remoteQueryEngine, RemoteQueryEngine.class);
      }
   }

   private boolean verifyChainContainsRemoteValueWrapperInterceptor(ComponentRegistry cr) {
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      return interceptorChain != null && interceptorChain.containsInterceptorType(RemoteValueWrapperInterceptor.class, true);
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      Configuration cfg = cr.getComponent(Configuration.class);
      removeRemoteIndexingInterceptorFromConfig(cfg);
   }

   private void removeRemoteIndexingInterceptorFromConfig(Configuration cfg) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

      for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
         if (!(interceptorConfig.interceptor() instanceof RemoteValueWrapperInterceptor)) {
            customInterceptorsBuilder.addInterceptor().read(interceptorConfig);
         }
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }
}
