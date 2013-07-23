package org.infinispan.integrationtests.cdijcache.interceptor.service;

import javax.cache.annotation.CacheRemoveAll;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CacheRemoveAllService {

   @CacheRemoveAll
   public void removeAll() {
   }

   @CacheRemoveAll(cacheName = "custom")
   public void removeAllWithCacheName() {
   }

   @CacheRemoveAll(cacheName = "custom")
   public void removeAllAfterInvocationWithException() {
      throw new RuntimeException();
   }

   @CacheRemoveAll(cacheName = "custom", afterInvocation = false)
   public void removeAllBeforeInvocationWithException() {
      throw new RuntimeException();
   }
}
