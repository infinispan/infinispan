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
import javax.cache.annotation.CacheResult;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheResultService {

   private int nbCall;

   public CacheResultService() {
      this.nbCall = 0;
   }

   @CacheResult
   public String cacheResult(String user) {
      nbCall++;
      return "Morning " + user;
   }

   @CacheResult(cacheName = "custom")
   public String cacheResultWithCacheName(String user) {
      nbCall++;
      return "Hi " + user;
   }

   @CacheResult(cacheName = "custom")
   public String cacheResultWithCacheKeyParam(@CacheKeyParam String user, String unused) {
      nbCall++;
      return "Hola " + user;
   }

   @CacheResult(cacheName = "custom", cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public String cacheResultWithCacheKeyGenerator(String user) {
      nbCall++;
      return "Hello " + user;
   }

   @CacheResult(cacheName = "custom", skipGet = true)
   public String cacheResultSkipGet(String user) {
      nbCall++;
      return "Hey " + user;
   }

   @CacheResult(cacheName = "small")
   public String cacheResultWithSpecificCacheManager(String user) {
      nbCall++;
      return "Bonjour " + user;
   }

   public int getNbCall() {
      return nbCall;
   }
}
