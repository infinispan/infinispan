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
package org.infinispan.cdi.interceptor;

import org.infinispan.Cache;

import java.lang.reflect.Method;

/**
 * This is the cache resolver contract used by interceptors to resolve a cache in function of it's name and the
 * annotated method.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public interface CacheResolver {
   /**
    * Resolves a cache in function of it's name and the annotated method (see {@linkplain
    * javax.cache.interceptor.CacheResult CacheResult}, {@linkplain javax.cache.interceptor.CacheRemoveEntry
    * CacheRemoveEntry} and {@linkplain javax.cache.interceptor.CacheRemoveAll CacheRemoveAll}).
    *
    * @param cacheName the cache name to resolve.
    * @param method    the method annotated with a cache annotation.
    * @param <K>       the cache key type.
    * @param <V>       the cache value type.
    * @return the resolved cache.
    */
   <K, V> Cache<K, V> resolveCache(String cacheName, Method method);
}
