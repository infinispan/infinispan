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
package org.infinispan.cdi.interceptors;

import org.infinispan.Cache;
import org.infinispan.cdi.util.Contracts;
import org.infinispan.manager.CacheContainer;

import javax.cache.CacheException;
import javax.cache.interceptor.CacheRemoveAll;
import javax.cache.interceptor.CacheRemoveEntry;
import javax.inject.Inject;
import java.lang.reflect.Method;

import static org.infinispan.cdi.util.CacheHelper.getDefaultMethodCacheName;

/**
 * <p>This is the default cache resolver implementation.</p>
 * <p>This resolver uses the algorithm defined by JSR-107. If
 * the given cache name is not specified the default cache name used for resolution is the fully qualified name of the
 * annotated method. If method is annotated with {@link CacheRemoveAll} or {@link CacheRemoveEntry} and the cache name
 * is not specified a {@link CacheException} is thrown.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultCacheResolver implements CacheResolver {

   private final CacheContainer cacheContainer;

   @Inject
   public DefaultCacheResolver(CacheContainer cacheContainer) {
      this.cacheContainer = cacheContainer;
   }

   @Override
   public <K, V> Cache<K, V> resolveCache(String cacheName, Method method) {
      Contracts.assertNotNull(cacheName, "cacheName parameter cannot be null");
      Contracts.assertNotNull(method, "method parameter cannot be null");

      // TODO KP: Not sure if an exception has to be thrown?
      // The spec says: How interpret this Unlike @CacheResult, @CacheEntryRemove there are not automatic cache names
      // for remove cache.
      if (cacheName.isEmpty() &&
            (method.isAnnotationPresent(CacheRemoveAll.class) || method.isAnnotationPresent(CacheRemoveEntry.class))) {

         throw new CacheException("Method named '" + method.getName() + "' annotated with CacheRemoveAll or " +
                                        "CacheRemoveEntry doesn't specify a cache name");
      }

      String name = cacheName.isEmpty() ? getDefaultMethodCacheName(method) : cacheName;
      return cacheContainer.getCache(name);
   }
}
