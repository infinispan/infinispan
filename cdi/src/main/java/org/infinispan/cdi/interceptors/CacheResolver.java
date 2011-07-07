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

import java.lang.reflect.Method;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public interface CacheResolver {
   /**
    * Resolves a cache in function of it's name and of the annotated method (see {@link
    * javax.cache.interceptor.CacheResult}, {@link javax.cache.interceptor.CacheRemoveEntry} and {@link
    * javax.cache.interceptor.CacheRemoveAll}).
    *
    * @param cacheName The cache name to resolve.
    * @param method    The method annotated with a cache annotation.
    * @param <K>       The cache key type.
    * @param <V>       The cache value type.
    * @return The resolved cache.
    */
   <K, V> Cache<K, V> resolveCache(String cacheName, Method method);
}
