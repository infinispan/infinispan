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
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.backend.QueryInterceptor;
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
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapperSearchWorkCreator;
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
      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerWrapper(ProtobufWrapper.INSTANCE);
      initProtobufMetadataManager(gcr.getGlobalConfiguration(), gcr);
   }

   private void initProtobufMetadataManager(GlobalConfiguration globalCfg, GlobalComponentRegistry gcr) {
      ProtobufMetadataManagerImpl protobufMetadataManager = new ProtobufMetadataManagerImpl();
      gcr.registerComponent(protobufMetadataManager, ProtobufMetadataManager.class);
      registerProtobufMetadataManagerMBean(protobufMetadataManager, gcr);

      SerializationContext serCtx = protobufMetadataManager.getSerializationContext();
      ClassLoader classLoader = globalCfg.classLoader();
      processProtostreamSerializationContextInitializers(classLoader, serCtx);

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerTranscoder(new ProtostreamJsonTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamTextTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamObjectTranscoder(serCtx));
      encoderRegistry.registerTranscoder(new ProtostreamBinaryTranscoder());
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
      GlobalComponentRegistry gcr = cr.getGlobalComponentRegistry();
      InternalCacheRegistry icr = gcr.getComponent(InternalCacheRegistry.class);
      if (!icr.isInternalCache(cacheName)) {
         ProtobufMetadataManagerImpl protobufMetadataManager =
            (ProtobufMetadataManagerImpl) gcr.getComponent(ProtobufMetadataManager.class);
         protobufMetadataManager.addCacheDependency(cacheName);
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
         ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) cr.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
         SerializationContext serCtx = protobufMetadataManager.getSerializationContext();

         RemoteQueryManager remoteQueryManager = buildQueryManager(cfg, serCtx, cr);

         Matcher matcher = remoteQueryManager.getMatcher();
         cr.registerComponent(matcher, matcher.getClass());
         cr.registerComponent(remoteQueryManager, RemoteQueryManager.class);

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
}
