/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.jcache.annotation;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.annotation.CacheInvocationContext;
import javax.cache.annotation.CacheResolver;
import javax.cache.spi.CachingProvider;
import javax.enterprise.context.ApplicationScoped;
import java.lang.annotation.Annotation;

import static org.infinispan.jcache.annotation.Contracts.assertNotNull;

/**
 * Default {@link javax.cache.annotation.CacheResolver} implementation for
 * standalone environments, where no Cache/CacheManagers are injected via CDI.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@ApplicationScoped
@SuppressWarnings("unused")
public class DefaultCacheResolver implements CacheResolver {

   private CacheManager defaultCacheManager;

   // Created by proxy
   @SuppressWarnings("unused")
   DefaultCacheResolver() {
      CachingProvider provider = Caching.getCachingProvider();
      defaultCacheManager = provider.getCacheManager(provider.getDefaultURI(), provider.getDefaultClassLoader());
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(CacheInvocationContext<? extends Annotation> cacheInvocationContext) {
      assertNotNull(cacheInvocationContext, "cacheInvocationContext parameter must not be null");

      final String cacheName = cacheInvocationContext.getCacheName();

      // If cache name is empty, default cache of default cache manager is returned
      if (cacheName.trim().isEmpty()) {
         return defaultCacheManager.configureCache(cacheName,
               new javax.cache.MutableConfiguration<K, V>());
      }

      for (Cache<?, ?> cache : defaultCacheManager.getCaches()) {
         if (cache.getName().equals(cacheName))
            return (Cache<K, V>) cache;
      }

      // If the cache has not been defined in the default cache manager or
      // in a specific one a new cache is created in the default cache manager
      // with the default configuration.
      return defaultCacheManager.configureCache(cacheName,
            new javax.cache.MutableConfiguration<K, V>());
   }

}
