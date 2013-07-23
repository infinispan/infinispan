package org.infinispan.integrationtests.cdijcache.interceptor.service;

import javax.cache.annotation.CacheKey;
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
   public void putWithCacheKeyParam(@CacheKey long id, long id2, @CacheValue String name) {
   }

   @CachePut(cacheName = "custom", afterInvocation = false)
   public void putBeforeInvocation(long id, @CacheValue String name) {
      throw new RuntimeException();
   }

   @CachePut(cacheName = "custom", cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public void putWithCacheKeyGenerator(long id, @CacheValue String name) {
   }
}
