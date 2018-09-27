package org.infinispan.query.impl;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryEngine;
import org.infinispan.query.dsl.embedded.impl.QueryCache;

/**
 * Lookup methods for various internal components of search module.
 *
 * @author Marko Luksa
 * @author Galder Zamarreño
 */
public final class ComponentRegistryUtils {

   private ComponentRegistryUtils() {
   }

   private static <T> T getRequiredComponent(Cache<?, ?> cache, Class<T> clazz) {
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      T component = componentRegistry.getComponent(clazz, clazz.getName());
      if (component == null) {
         throw new IllegalStateException(clazz.getName() + " not found in component registry");
      }
      return component;
   }

   private static void ensureIndexed(Cache<?, ?> cache) {
      Configuration cfg = SecurityActions.getCacheConfiguration(cache);
      if (!cfg.indexing().index().isEnabled()) {
         throw new IllegalStateException("Indexing was not enabled on cache " + cache.getName());
      }
   }

   public static SearchIntegrator getSearchIntegrator(Cache<?, ?> cache) {
      ensureIndexed(cache);
      return getRequiredComponent(cache, SearchIntegrator.class);
   }

   public static QueryInterceptor getQueryInterceptor(Cache<?, ?> cache) {
      ensureIndexed(cache);
      return getRequiredComponent(cache, QueryInterceptor.class);
   }

   public static KeyTransformationHandler getKeyTransformationHandler(Cache<?, ?> cache) {
      ensureIndexed(cache);
      return getRequiredComponent(cache, KeyTransformationHandler.class);
   }

   public static EmbeddedQueryEngine getEmbeddedQueryEngine(Cache<?, ?> cache) {
      return getRequiredComponent(cache, EmbeddedQueryEngine.class);
   }

   public static TimeService getTimeService(Cache<?, ?> cache) {
      return getRequiredComponent(cache, TimeService.class);
   }

   /**
    * Returns the optional QueryCache.
    */
   public static QueryCache getQueryCache(Cache<?, ?> cache) {
      return SecurityActions.getCacheGlobalComponentRegistry(cache.getAdvancedCache()).getComponent(QueryCache.class);
   }
}
