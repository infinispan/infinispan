package org.infinispan.query.remote.impl;

import java.util.Collection;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.CustomInterceptorsConfigurationBuilder;
import org.infinispan.configuration.cache.InterceptorConfiguration;
import org.infinispan.configuration.cache.InterceptorConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.BatchingInterceptor;
import org.infinispan.interceptors.impl.InvocationContextInterceptor;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.BaseProtoStreamMarshaller;
import org.infinispan.query.remote.client.ProtostreamSerializationContextInitializer;
import org.infinispan.query.remote.impl.dataconversion.ProtostreamBinaryTranscoder;
import org.infinispan.query.remote.impl.dataconversion.ProtostreamJsonTranscoder;
import org.infinispan.query.remote.impl.dataconversion.ProtostreamObjectTranscoder;
import org.infinispan.query.remote.impl.dataconversion.ProtostreamTextTranscoder;
import org.infinispan.query.remote.impl.filter.ContinuousQueryResultExternalizer;
import org.infinispan.query.remote.impl.filter.FilterResultExternalizer;
import org.infinispan.query.remote.impl.filter.IckleBinaryProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.filter.IckleContinuousQueryProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.impl.filter.IckleProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.impl.filter.IckleProtobufFilterAndConverter;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapperInterceptor;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.kohsuke.MetaInfServices;

/**
 * Initializes components for remote query. Each cache manager has its own instance of this class during its lifetime.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public final class LifecycleManager implements ModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   /**
    * Caching the looked-up MBeanServer for the lifetime of the cache manager is safe.
    */
   private MBeanServer mbeanServer;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.PROTOBUF_VALUE_WRAPPER, new ProtobufValueWrapper.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_PROTOBUF_CACHE_EVENT_FILTER_CONVERTER, new IckleProtobufCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_PROTOBUF_FILTER_AND_CONVERTER, new IckleProtobufFilterAndConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CONTINUOUS_QUERY_CACHE_EVENT_FILTER_CONVERTER, new IckleContinuousQueryProtobufCacheEventFilterConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_BINARY_PROTOBUF_FILTER_AND_CONVERTER, new IckleBinaryProtobufFilterAndConverter.Externalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_CONTINUOUS_QUERY_RESULT, new ContinuousQueryResultExternalizer());
      externalizerMap.put(ExternalizerIds.ICKLE_FILTER_RESULT, new FilterResultExternalizer());
      externalizerMap.put(ExternalizerIds.REMOTE_QUERY_DEFINITION, new RemoteQueryDefinitionExternalizer());
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerWrapper(ProtobufWrapper.INSTANCE);
      initProtobufMetadataManager((DefaultCacheManager) cacheManager, gcr);
   }

   private void initProtobufMetadataManager(DefaultCacheManager cacheManager, GlobalComponentRegistry gcr) {
      ProtobufMetadataManagerImpl protobufMetadataManager = new ProtobufMetadataManagerImpl();
      gcr.registerComponent(protobufMetadataManager, ProtobufMetadataManager.class);
      registerProtobufMetadataManagerMBean(protobufMetadataManager, gcr);
      ClassLoader classLoader = cacheManager.getCacheManagerConfiguration().classLoader();
      processContextInitializers(classLoader, protobufMetadataManager);

      SerializationContext serCtx = protobufMetadataManager.getSerializationContext();
      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerTranscoder(new ProtostreamJsonTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamTextTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamObjectTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamBinaryTranscoder());
   }

   private void processContextInitializers(ClassLoader classLoader, ProtobufMetadataManagerImpl metadataManager) {
      Collection<ProtostreamSerializationContextInitializer> initializers =
            ServiceFinder.load(ProtostreamSerializationContextInitializer.class, classLoader);

      initializers.forEach(initCtx -> {
         try {
            initCtx.init(metadataManager.getSerializationContext());
         } catch (Exception e) {
            throw log.errorInitializingSerCtx(e);
         }
      });
   }

   private void registerProtobufMetadataManagerMBean(ProtobufMetadataManagerImpl protobufMetadataManager, GlobalComponentRegistry gcr) {
      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      if (mbeanServer == null) {
         mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
      }

      String groupName = "type=RemoteQuery,name=" + ObjectName.quote(globalCfg.globalJmxStatistics().cacheManagerName());
      String jmxDomain = JmxUtil.buildJmxDomain(globalCfg, mbeanServer, groupName);
      ComponentMetadataRepo metadataRepo = gcr.getComponentMetadataRepo();
      ManageableComponentMetadata metadata = metadataRepo.findComponentMetadata(ProtobufMetadataManagerImpl.class)
            .toManageableComponentMetadata();
      try {
         ResourceDMBean mBean = new ResourceDMBean(protobufMetadataManager, metadata);
         ObjectName objName = new ObjectName(jmxDomain + ":" + groupName + ",component=" + metadata.getJmxObjectName());
         protobufMetadataManager.setObjectName(objName);
         JmxUtil.registerMBean(mBean, objName, mbeanServer);
      } catch (Exception e) {
         throw new CacheException("Unable to register ProtobufMetadataManager MBean", e);
      }
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      unregisterProtobufMetadataManagerMBean(gcr);
   }

   private void unregisterProtobufMetadataManagerMBean(GlobalComponentRegistry gcr) {
      if (mbeanServer != null) {
         try {
            ObjectName objName = gcr.getComponent(ProtobufMetadataManager.class).getObjectName();
            JmxUtil.unregisterMBean(objName, mbeanServer);
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
      GlobalComponentRegistry gcr = cr.getGlobalComponentRegistry();
      InternalCacheRegistry icr = gcr.getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class);
         protobufMetadataManager.addCacheDependency(cacheName);

         if (cfg.indexing().index().isEnabled()) {
            log.infof("Registering ProtobufValueWrapperInterceptor for cache %s", cacheName);
            EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
            createProtobufValueWrapperInterceptor(cr, cfg, cacheManager);
         }
      }
   }

   private void createProtobufValueWrapperInterceptor(ComponentRegistry cr, Configuration cfg, EmbeddedCacheManager cacheManager) {
      ProtobufValueWrapperInterceptor wrapperInterceptor = cr.getComponent(ProtobufValueWrapperInterceptor.class);
      if (wrapperInterceptor == null) {
         wrapperInterceptor = new ProtobufValueWrapperInterceptor(cacheManager);

         // Interceptor registration not needed, core configuration handling
         // already does it for all custom interceptors - UNLESS the InterceptorChain already exists in the component registry!
         AsyncInterceptorChain ic = cr.getComponent(AsyncInterceptorChain.class);

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
            cr.registerComponent(wrapperInterceptor, ProtobufValueWrapperInterceptor.class);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   private RemoteQueryManager buildQueryManager(Configuration cfg, SerializationContext ctx, ComponentRegistry cr) {
      ContentTypeConfiguration valueEncoding = cfg.encoding().valueDataType();
      boolean compatEnabled = cfg.compatibility().enabled();
      if (!compatEnabled) {
         if (valueEncoding != null) {
            if (!valueEncoding.isEncodingChanged() || valueEncoding.mediaType() != null && valueEncoding.mediaType().equals(MediaType.APPLICATION_PROTOSTREAM)) {
               return new ProtobufRemoteQueryManager(ctx, cr);
            }
         }
         return new GenericCompatRemoteQueryManager(cr);

      } else {
         Marshaller compatMarshaller = cfg.compatibility().marshaller();
         if (compatMarshaller instanceof BaseProtoStreamMarshaller) {
            return new ProtostreamCompatRemoteQueryManager(cr);
         }
         return new GenericCompatRemoteQueryManager(cr);
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      InternalCacheRegistry icr = cr.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         Configuration cfg = cr.getComponent(Configuration.class);
         if (cfg.indexing().index().isEnabled()) {
            if (!verifyChainContainsProtobufValueWrapperInterceptor(cr)) {
               throw new IllegalStateException("It was expected to find the ProtobufValueWrapperInterceptor registered in the InterceptorChain but it wasn't found");
            }
         } else if (verifyChainContainsProtobufValueWrapperInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the ProtobufValueWrapperInterceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }

         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         SerializationContext serCtx = protobufMetadataManager.getSerializationContext();

         RemoteQueryManager remoteQueryManager = buildQueryManager(cfg, serCtx, cr);

         Matcher matcher = remoteQueryManager.getMatcher();
         cr.registerComponent(matcher, matcher.getClass());
         cr.registerComponent(remoteQueryManager, RemoteQueryManager.class);
      }
   }

   private boolean verifyChainContainsProtobufValueWrapperInterceptor(ComponentRegistry cr) {
      AsyncInterceptorChain interceptorChain = cr.getComponent(AsyncInterceptorChain.class);
      return interceptorChain != null && interceptorChain.containsInterceptorType(ProtobufValueWrapperInterceptor.class, true);
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      Configuration cfg = cr.getComponent(Configuration.class);
      removeProtobufValueWrapperInterceptor(cfg);
   }

   private void removeProtobufValueWrapperInterceptor(Configuration cfg) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      CustomInterceptorsConfigurationBuilder customInterceptorsBuilder = builder.customInterceptors();

      for (InterceptorConfiguration interceptorConfig : cfg.customInterceptors().interceptors()) {
         if (!(interceptorConfig.asyncInterceptor() instanceof ProtobufValueWrapperInterceptor)) {
            customInterceptorsBuilder.addInterceptor().read(interceptorConfig);
         }
      }

      cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
   }

   @Override
   public void cacheManagerStopped(GlobalComponentRegistry gcr) {
      mbeanServer = null;
   }
}
