package org.infinispan.query.remote;

import com.google.protobuf.Descriptors;
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
import org.infinispan.interceptors.BatchingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.MarshallerRegistration;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.query.remote.indexing.RemoteValueWrapperInterceptor;
import org.infinispan.query.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class LifecycleManager extends AbstractModuleLifecycle {

   private static final Log log = LogFactory.getLog(LifecycleManager.class, Log.class);

   private boolean isSerializationContextInitialized = false;

   private void initSerializationContext() {
      if (!isSerializationContextInitialized) {
         isSerializationContextInitialized = true;
         try {
            SerializationContext serCtx = SerializationContextHolder.getSerializationContext();
            MarshallerRegistration.registerMarshallers(serCtx);
         } catch (IOException e) {
            throw new CacheException("Failed to initialise serialization context", e);
         } catch (Descriptors.DescriptorValidationException e) {
            throw new CacheException("Failed to initialise serialization context", e);
         }
      }
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) {
      initSerializationContext();

      Map<Integer, AdvancedExternalizer<?>> externalizerMap = globalCfg.serialization().advancedExternalizers();
      externalizerMap.put(ExternalizerIds.PROTOBUF_VALUE_WRAPPER, new ProtobufValueWrapper.Externalizer());
   }

   /**
    * Registers the remote indexing interceptor in the cache before it gets started
    */
   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration cfg, String cacheName) {
      if (cfg.indexing().enabled()) {
         log.infof("Registering RemoteValueWrapperInterceptor for cache %s", cacheName);
         createRemoteIndexingInterceptor(cr, cfg);
      }
   }

   private void createRemoteIndexingInterceptor(ComponentRegistry cr, Configuration cfg) {
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
      boolean indexingEnabled = configuration.indexing().enabled();
      if (!indexingEnabled) {
         if (verifyChainContainsRemoteIndexingInterceptor(cr)) {
            throw new IllegalStateException("It was NOT expected to find the RemoteValueWrapperInterceptor registered in the InterceptorChain as indexing was disabled, but it was found");
         }
         return;
      }
      if (!verifyChainContainsRemoteIndexingInterceptor(cr)) {
         throw new IllegalStateException("It was expected to find the RemoteValueWrapperInterceptor registered in the InterceptorChain but it wasn't found");
      }
   }

   private boolean verifyChainContainsRemoteIndexingInterceptor(ComponentRegistry cr) {
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
