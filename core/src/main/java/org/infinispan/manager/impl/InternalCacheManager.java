package org.infinispan.manager.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * @since 15.0
 **/
@SurvivesRestarts
public abstract class InternalCacheManager implements EmbeddedCacheManager {

   public static GlobalComponentRegistry of(EmbeddedCacheManager cacheManager) {
      return ((InternalCacheManager) cacheManager).getGlobalComponentRegistry();
   }

   protected GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponentRegistry();
   }

   /**
    * Registers and starts the named cache.
    *
    * <p>
    * Registration is synchronous, so {@link EmbeddedCacheManager#cacheExists(String)} reports the cache as soon as this
    * method returns; the start and its initial state transfer run on the supplied {@link BlockingManager}.
    * </p>
    *
    * @param ecm the cache manager on which to create the cache.
    * @param cacheName the name of the cache to create; its configuration must already be defined.
    * @param blockingManager the blocking manager used to offload the cache start off the calling thread.
    * @return a stage that completes when the cache has started, or completes exceptionally if the start fails.
    * @throws CacheConfigurationException if no configuration is defined for {@code cacheName}, or it is a template.
    * @throws SecurityException if authorization is enabled for the cache and the subject lacks {@code LIFECYCLE}.
    * @throws IllegalLifecycleStateException if the cache manager is stopping or stopped.
    */
   public static CompletionStage<Void> createCache(EmbeddedCacheManager ecm, String cacheName, BlockingManager blockingManager) {
      return ((InternalCacheManager) ecm).internalCreateCache(cacheName, blockingManager);
   }

   /**
    * Registers the named cache and starts it asynchronously.
    *
    * <p>
    * Registration happens before this method returns, so {@link #cacheExists(String)} reports the cache from then on.
    * The start, including the initial state transfer when {@code await-initial-transfer} is enabled, runs on
    * {@code blockingManager}; this method does not wait for it. If the cache is already registered, no new start is
    * submitted and the returned stage follows the existing one.
    * </p>
    *
    * @param cacheName the name of the cache to create; its configuration must already be defined.
    * @param blockingManager the blocking manager that runs the cache start.
    * @return a stage that completes when the cache has started, or completes exceptionally if the start fails.
    * @throws CacheConfigurationException if no configuration is defined for {@code cacheName}, or it is a template.
    * @throws SecurityException if authorization is enabled for the cache and the subject lacks {@code LIFECYCLE}.
    * @throws IllegalLifecycleStateException if the cache manager is stopping or stopped.
    */
   protected abstract CompletionStage<Void> internalCreateCache(String cacheName, BlockingManager blockingManager);

   protected abstract GlobalComponentRegistry globalComponentRegistry();
}
