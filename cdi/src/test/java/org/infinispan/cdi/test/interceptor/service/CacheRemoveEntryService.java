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

import javax.cache.interceptor.CacheKeyParam;
import javax.cache.interceptor.CacheRemoveEntry;

import static org.infinispan.cdi.util.Contracts.assertNotNull;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheRemoveEntryService {

   @CacheRemoveEntry
   public void removeEntry(String login) {
      assertNotNull(login, "login parameter cannot be null");
   }

   @CacheRemoveEntry(cacheName = "custom")
   public void removeEntryWithCacheName(String login) {
      assertNotNull(login, "login parameter cannot be null");
   }

   @CacheRemoveEntry(cacheName = "custom")
   public void removeEntryWithCacheKeyParam(@CacheKeyParam String login, String unused) {
      assertNotNull(login, "login parameter cannot be null");
   }

   @CacheRemoveEntry(cacheName = "custom", afterInvocation = false)
   public void removeEntryBeforeInvocationWithException(String login) {
      assertNotNull(login, "login parameter cannot be null");
   }

   @CacheRemoveEntry(cacheName = "custom", cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public void removeEntryWithCacheKeyGenerator(String login) {
      assertNotNull(login, "login parameter cannot be null");
   }
}
