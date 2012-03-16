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
package org.infinispan.cdi.test.interceptor.service;

import javax.cache.annotation.CacheKeyParam;
import javax.cache.annotation.CachePut;
import javax.cache.annotation.CacheValue;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CachePutService {

   @CachePut
   public void put(long id, @CacheValue String name) {
   }

   @CachePut(cacheName = "custom")
   public void putWithCacheName(long id, @CacheValue String name) {
   }

   @CachePut(cacheName = "custom")
   public void putWithCacheKeyParam(@CacheKeyParam long id, long id2, @CacheValue String name) {
   }

   @CachePut(cacheName = "custom", afterInvocation = false)
   public void putBeforeInvocation(long id, @CacheValue String name) {
      throw new RuntimeException();
   }

   @CachePut(cacheName = "custom", cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public void putWithCacheKeyGenerator(long id, @CacheValue String name) {
   }
}
