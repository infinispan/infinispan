package org.infinispan.jcache.remote.annotation;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Iterator;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.annotation.CacheInvocationContext;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.inject.Inject;

import org.infinispan.cdi.InfinispanExtension;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.jcache.annotation.Contracts;
import org.infinispan.jcache.annotation.InjectedCacheResolver;
import org.infinispan.jcache.remote.JCacheManager;

public class RemoteInjectedCacheResolver implements InjectedCacheResolver {
   private RemoteCacheManager defaultCacheManager;
   private JCacheManager defaultJCacheManager;

   // for proxy.
   public RemoteInjectedCacheResolver() {
   }

   @Inject
   public RemoteInjectedCacheResolver(final InfinispanExtension extension, final BeanManager beanManager) {
      defaultCacheManager = getBeanReference(beanManager, RemoteCacheManager.class);
      defaultJCacheManager = toJCacheManager(defaultCacheManager);
   }

   private JCacheManager toJCacheManager(final RemoteCacheManager cacheManager) {
      String name = String.format("RemoteCacheManager@%xd", System.identityHashCode(cacheManager));
      return new JCacheManager(URI.create(name), cacheManager, Caching.getCachingProvider());
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(final CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
      Contracts.assertNotNull(cacheInvocationContext, "cacheInvocationContext parameter must not be null");
      return getCacheFromDefaultCacheManager(cacheInvocationContext.getCacheName());
   }

   private <K, V> Cache<K, V> getCacheFromDefaultCacheManager(final String cacheName) {
      return defaultJCacheManager.getOrCreateCache(cacheName, defaultCacheManager.<K, V> getCache(cacheName));
   }

   @SuppressWarnings("unchecked")
   private <T> T getBeanReference(BeanManager beanManager, final Class<T> beanType) {
      final Iterator<Bean<?>> iterator = beanManager.getBeans(beanType).iterator();
      if (!iterator.hasNext()) {
         throw new IllegalStateException(String.format(
               "Default bean of type %s not found.", beanType.getName()));
      }

      final Bean<?> configurationBean = iterator.next();
      final CreationalContext<?> createCreationalContext = beanManager.createCreationalContext(configurationBean);
      return (T) beanManager.getReference(configurationBean, beanType, createCreationalContext);
   }

}
