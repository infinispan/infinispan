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
package org.infinispan.cdi.test.interceptors.service;


import org.infinispan.cdi.test.interceptors.service.generator.CustomCacheKeyGenerator;

import javax.cache.interceptor.CacheRemoveAll;
import javax.cache.interceptor.CacheRemoveEntry;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class AdminService {

   @CacheRemoveEntry
   public void removeUser(String login) {
      if (login == null) {
         throw new IllegalArgumentException("login parameter cannot be null");
      }
   }

   @CacheRemoveEntry(afterInvocation = false)
   public void removeUserBeforeInvocation(String login) {
      removeUser(login);
   }

   @CacheRemoveEntry(cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public void removeUserWithCustomCacheKeyGenerator(String login) {
      removeUser(login);
   }

   @CacheRemoveEntry(cacheName = "custom")
   public void removeUserWithCacheName(String login) {
      removeUser(login);
   }

   @CacheRemoveAll
   public void removeAllUser() {
   }

   @CacheRemoveAll(afterInvocation = false)
   public void removeAllUserBeforeInvocation(String login) {
      throw new IllegalArgumentException();
   }

   @CacheRemoveAll(cacheName = "custom")
   public void removeAllUserWithCacheName(String login) {
      removeUser(login);
   }
}
