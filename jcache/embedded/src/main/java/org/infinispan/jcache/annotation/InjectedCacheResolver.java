package org.infinispan.jcache.annotation;

import org.infinispan.cdi.InfinispanExtension;
import org.infinispan.cdi.InfinispanExtensionEmbedded;
import org.infinispan.cdi.util.BeanManagerProvider;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.jcache.embedded.JCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.annotation.CacheInvocationContext;
import javax.cache.annotation.CacheResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Injected cache resolver for situations where caches and/or cache managers
 * are injected into the CDI beans. In these situations, bridging is required
 * in order to bridge between the Infinispan based caches and the JCache
 * cache instances which is what it's expected by the specification cache
 * resolver.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@ApplicationScoped
public class InjectedCacheResolver implements CacheResolver {

   private EmbeddedCacheManager defaultCacheManager;

   private final Map<EmbeddedCacheManager, JCacheManager> jcacheManagers = new HashMap<>();
   private JCacheManager defaultJCacheManager;

   // for proxy.
   public InjectedCacheResolver() {
   }

   @Inject
   public InjectedCacheResolver(final InfinispanExtension extension, final BeanManager beanManager) {
      final Set<InfinispanExtensionEmbedded.InstalledCacheManager> installedCacheManagers = extension.getEmbeddedExtension().getInstalledEmbeddedCacheManagers(beanManager);
      for (final InfinispanExtensionEmbedded.InstalledCacheManager installedCacheManager : installedCacheManagers) {
         final JCacheManager jcacheManager = toJCacheManager(installedCacheManager.getCacheManager());
         this.jcacheManagers.put(installedCacheManager.getCacheManager(), jcacheManager);
      }
      initializeDefaultCacheManagers();
   }

   private void initializeDefaultCacheManagers() {
      defaultCacheManager = getBeanReference(EmbeddedCacheManager.class);

      if (jcacheManagers.containsKey(defaultCacheManager)) {
         defaultJCacheManager = jcacheManagers.get(defaultCacheManager);
      } else {
         defaultJCacheManager = toJCacheManager(defaultCacheManager);
         jcacheManagers.put(defaultCacheManager, defaultJCacheManager);
      }
   }

   private JCacheManager toJCacheManager(final EmbeddedCacheManager cacheManager) {
      final GlobalConfiguration globalCfg = cacheManager.getCacheManagerConfiguration();
      final String name = globalCfg.globalJmxStatistics().cacheManagerName();
      return new JCacheManager(URI.create(name), cacheManager, Caching.getCachingProvider());
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(final CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
      Contracts.assertNotNull(cacheInvocationContext, "cacheInvocationContext parameter must not be null");

      final String cacheName = cacheInvocationContext.getCacheName();

      // If the cache name is empty the default cache of the default cache manager is returned.
      if (cacheName.trim().isEmpty()) {
         return getCacheFromDefaultCacheManager(cacheName);
      }

      // Iterate on all cache managers because the cache used by the
      // interceptor could use a specific cache manager.
      for (final EmbeddedCacheManager cm : jcacheManagers.keySet()) {
         final Set<String> cacheNames = cm.getCacheNames();
         for (final String name : cacheNames) {
            if (name.equals(cacheName)) {
               final JCacheManager jcacheManager = jcacheManagers.get(cm);
               final Cache<K, V> cache = jcacheManager.getCache(cacheName);
               if (cache != null)
                  return cache;

               return jcacheManager.getOrCreateCache(
                     cacheName, cm.<K, V>getCache(cacheName).getAdvancedCache());
            }
         }
      }

      // If the cache has not been defined in the default cache manager
      // or in a specific one a new cache is created in the default
      // cache manager with the default configuration.
      return getCacheFromDefaultCacheManager(cacheName);
   }

   private <K, V> Cache<K, V> getCacheFromDefaultCacheManager(final String cacheName) {
      final Configuration defaultInjectedConfiguration = getBeanReference(Configuration.class);
      defaultCacheManager.defineConfiguration(cacheName, defaultInjectedConfiguration);
      return defaultJCacheManager.getOrCreateCache(cacheName, defaultCacheManager.<K, V> getCache(cacheName)
            .getAdvancedCache());
   }

   private BeanManager getBeanManager() {
      return BeanManagerProvider.getInstance().getBeanManager();
   }

   @SuppressWarnings("unchecked")
   private <T> T getBeanReference(final Class<T> beanType) {
      final BeanManager bm = getBeanManager();
      final Iterator<Bean<?>> iterator = bm.getBeans(beanType).iterator();
      if (!iterator.hasNext()) {
         throw new IllegalStateException(String.format(
               "Default bean of type %s not found.", beanType.getName()));
      }

      final Bean<?> configurationBean = iterator.next();
      final CreationalContext<?> createCreationalContext = bm.createCreationalContext(configurationBean);
      return (T) bm.getReference(configurationBean, beanType, createCreationalContext);
   }

}