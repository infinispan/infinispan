package org.infinispan.query.impl;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.backend.LocalQueryInterceptor;
import org.infinispan.query.backend.QueryInterceptor;

/**
 * Component registry utilities
 *
 * @author Marko Luksa
 * @author Galder Zamarreño
 */
public class ComponentRegistryUtils {

   private ComponentRegistryUtils() {
   }

   public static <T> T getComponent(Cache<?, ?> cache, Class<T> class1) {
      return getComponent(cache, class1, class1.getName());
   }

   public static <T> T getComponent(Cache<?, ?> cache, Class<T> class1, String name) {
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache());
      T component = componentRegistry.getComponent(class1, name);
      if (component == null) {
         throw new IllegalArgumentException("Indexing was not enabled on this cache. " + class1 + " not found in registry");
      }
      return component;
   }

   public static QueryInterceptor getQueryInterceptor(Cache<?, ?> cache) {
      Class<? extends QueryInterceptor> queryType = SecurityActions.getCacheConfiguration(cache.getAdvancedCache()).indexing().indexLocalOnly()
            ? LocalQueryInterceptor.class : QueryInterceptor.class;
      return getComponent(cache, queryType);
   }

}
