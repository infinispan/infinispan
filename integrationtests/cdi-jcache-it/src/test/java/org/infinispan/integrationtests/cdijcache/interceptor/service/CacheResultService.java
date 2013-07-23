package org.infinispan.integrationtests.cdijcache.interceptor.service;

import javax.cache.annotation.CacheKey;
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
   public String cacheResultWithCacheKeyParam(@CacheKey String user, String unused) {
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
