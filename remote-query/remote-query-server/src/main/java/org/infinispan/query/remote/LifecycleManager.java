package org.infinispan.query.remote;

import org.infinispan.commons.CacheException;
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
import org.infinispan.query.remote.filter.JPAProtobufCacheEventFilterConverter;
import org.infinispan.query.remote.filter.JPAProtobufFilterAndConverter;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.indexing.RemoteValueWrapperInterceptor;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;
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
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      EmbeddedCacheManager cacheManager = gcr.getComponent(EmbeddedCacheManager.class);
      initProtobufMetadataManager((DefaultCacheManager) cacheManager, gcr);
   }

   private void initProtobufMetadataManager(DefaultCacheManager cacheManager, GlobalComponentRegistry gcr) {
      ProtobufMetadataManager protobufMetadataManager = new ProtobufMetadataManager();
      gcr.registerComponent(protobufMetadataManager, ProtobufMetadataManager.class);
      registerProtobufMetadataManagerMBean(protobufMetadataManager, gcr, cacheManager.getName());
   }

   private void registerProtobufMetadataManagerMBean(ProtobufMetadataManager protobufMetadataManager, GlobalComponentRegistry gcr, String cacheManagerName) {
      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      MBeanServer mBeanServer = JmxUtil.lookupMBeanServer(globalCfg);

      String groupName = "type=RemoteQuery,name=" + ObjectName.quote(cacheManagerName);
      String jmxDomain = JmxUtil.buildJmxDomain(globalCfg, mBeanServer, groupName);
      ComponentMetadataRepo metadataRepo = gcr.getComponentMetadataRepo();
      ManageableComponentMetadata metadata = metadataRepo.findComponentMetadata(ProtobufMetadataManager.class)
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
      if (!cacheName.equals(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME)) {
         ProtobufMetadataManager protobufMetadataManager = cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);

         // ensure the protobuf metadata cache is created
         protobufMetadataManager.getCache();

         cr.registerComponent(new ProtobufMatcher(protobufMetadataManager.getSerializationContext()), ProtobufMatcher.class);

         if (cfg.compatibility().enabled()) {
            cr.registerComponent(new CompatibilityReflectionMatcher(protobufMetadataManager.getSerializationContext()), CompatibilityReflectionMatcher.class);
         }

         if (cfg.indexing().index().isEnabled() && !cfg.compatibility().enabled()) {
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
            cr.registerComponent(wrapperInterceptor, wrapperInterceptor.getClass().getName(), true);
         }
         cfg.customInterceptors().interceptors(builder.build().customInterceptors().interceptors());
      }
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      Configuration configuration = cr.getComponent(Configuration.class);
      boolean remoteValueWrappingEnabled = configuration.indexing().index().isEnabled() && !configuration.compatibility().enabled();
      if (!remoteValueWrappingEnabled) {
         if (verifyChainContainsRemoteValueWrapperInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the RemoteValueWrapperInterceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }
         return;
      }
      if (!verifyChainContainsRemoteValueWrapperInterceptor(cr)) {
         throw new IllegalStateException("It was expected to find the RemoteValueWrapperInterceptor registered in the InterceptorChain but it wasn't found");
      }
   }

   private boolean verifyChainContainsRemoteValueWrapperInterceptor(ComponentRegistry cr) {
      InterceptorChain interceptorChain = cr.getComponent(InterceptorChain.class);
      return interceptorChain.containsInterceptorType(RemoteValueWrapperInterceptor.class, true);
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
