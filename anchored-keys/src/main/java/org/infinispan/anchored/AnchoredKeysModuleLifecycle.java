package org.infinispan.anchored;

import org.infinispan.Cache;
import org.infinispan.anchored.configuration.AnchoredKeysConfiguration;
import org.infinispan.anchored.impl.AnchoredKeysInterceptor;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.DynamicModuleMetadataProvider;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.statetransfer.StateTransferInterceptor;

/**
 * Install the required components for stable distribution caches.
 *
 * @author Dan Berindei
 * @since 11
 */
@InfinispanModule(name = "anchored-keys", requiredModules = "core")
public final class AnchoredKeysModuleLifecycle implements ModuleLifecycle, DynamicModuleMetadataProvider {

   private GlobalComponentRegistry gcr;
   private GlobalConfiguration globalConfiguration;

   @Override
   public void registerDynamicMetadata(ModuleMetadataBuilder.ModuleBuilder moduleBuilder, GlobalConfiguration globalConfiguration) {
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      this.gcr = gcr;
      this.globalConfiguration = globalConfiguration;
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      AnchoredKeysConfiguration anchoredKeysConfiguration =
            configuration.module(AnchoredKeysConfiguration.class);
      if (anchoredKeysConfiguration == null || !anchoredKeysConfiguration.enabled())
         return;

      // Configure and start the ownership cache
      String ownershipCacheName = AnchoredKeysInterceptor.ANCHOR_CACHE_PREFIX + cacheName;
      InternalCacheRegistry internalCacheRegistry = gcr.getComponent(InternalCacheRegistry.class);
      ConfigurationBuilder anchorCacheBuilder = new ConfigurationBuilder();
      anchorCacheBuilder.clustering().cacheMode(CacheMode.REPL_SYNC)
                        .stateTransfer().awaitInitialTransfer(false)
                        .jmxStatistics().enabled(false);
      // Use off-heap for the anchors if the real cache is also using off-heap
      // Otherwise keep the reference to the cached address
      if (configuration.memory().storageType() == StorageType.OFF_HEAP) {
         anchorCacheBuilder.memory().storageType(StorageType.OFF_HEAP);
      }
      internalCacheRegistry.registerInternalCache(ownershipCacheName, anchorCacheBuilder.build());

      AsyncInterceptorChain interceptorChain = cr.getComponent(AsyncInterceptorChain.class);
      interceptorChain.removeInterceptor(StateTransferInterceptor.class);
      interceptorChain.removeInterceptor(InvalidationInterceptor.class);

      // withEncoding always returns a Cache<?, ?>, which can't do put(Object, Object)
      Cache anchorCache = gcr.getCacheManager().getCache(ownershipCacheName)
            .getAdvancedCache().withKeyEncoding(IdentityEncoder.class);
      AnchoredKeysInterceptor interceptor = new AnchoredKeysInterceptor(anchorCache);
      cr.registerComponent(interceptor, AnchoredKeysInterceptor.class);
      assert interceptorChain.addInterceptorBefore(interceptor, CallInterceptor.class);
   }
}
