package org.infinispan.anchored;

import org.infinispan.anchored.configuration.AnchoredKeysConfiguration;
import org.infinispan.anchored.impl.AnchorManager;
import org.infinispan.anchored.impl.AnchoredDistributionInterceptor;
import org.infinispan.anchored.impl.AnchoredFetchInterceptor;
import org.infinispan.anchored.impl.AnchoredStateProvider;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.DynamicModuleMetadataProvider;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.interceptors.impl.ClusteringInterceptor;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.statetransfer.StateProvider;

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

      assert configuration.clustering().cacheMode().isReplicated();
      assert !configuration.clustering().stateTransfer().awaitInitialTransfer();

      cr.registerComponent(new AnchorManager(), AnchorManager.class);

      AsyncInterceptorChain interceptorChain = cr.getComponent(AsyncInterceptorChain.class);
      interceptorChain.removeInterceptor(ClusteringInterceptor.class);

      AnchoredDistributionInterceptor distInterceptor = new AnchoredDistributionInterceptor();
      cr.registerComponent(distInterceptor, AnchoredDistributionInterceptor.class);
      assert interceptorChain.addInterceptorBefore(distInterceptor, CallInterceptor.class);

      AnchoredFetchInterceptor fetchInterceptor = new AnchoredFetchInterceptor();
      cr.registerComponent(fetchInterceptor, AnchoredFetchInterceptor.class);
      assert interceptorChain.addInterceptorBefore(fetchInterceptor, CallInterceptor.class);

      BasicComponentRegistry bcr = cr.getComponent(BasicComponentRegistry.class);
      bcr.replaceComponent(StateProvider.class.getName(), new AnchoredStateProvider(), true);
      cr.rewire();
   }

}
