package org.infinispan.integrationtests.cdijcache.interceptor.service;

import static org.infinispan.cdi.common.util.Contracts.assertNotNull;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheRemove;

/**
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
 */
public class CacheRemoveEntryService {

   @CacheRemove
   public void removeEntry(String login) {
      assertNotNull(login, "login parameter must not be null");
   }

   @CacheRemove(cacheName = "custom")
   public void removeEntryWithCacheName(String login) {
      assertNotNull(login, "login parameter must not be null");
   }

   @CacheRemove(cacheName = "custom")
   public void removeEntryWithCacheKeyParam(@CacheKey String login, String unused) {
      assertNotNull(login, "login parameter must not be null");
   }

   @CacheRemove(cacheName = "custom", afterInvocation = false)
   public void removeEntryBeforeInvocationWithException(String login) {
      assertNotNull(login, "login parameter must not be null");
   }

   @CacheRemove(cacheName = "custom", cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public void removeEntryWithCacheKeyGenerator(String login) {
      assertNotNull(login, "login parameter must not be null");
   }
}
