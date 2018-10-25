package org.infinispan.integrationtests.cdijcache.interceptor.service;

import javax.cache.annotation.CacheKey;
import javax.cache.annotation.CacheResult;

/**
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
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

   @CacheResult
   public String defaultCacheResult1(final String name) {
      return getDefaultCacheResult(name);
   }

   @CacheResult
   public String defaultCacheResult2(final String name) {
      return getDefaultCacheResult(name);
   }

   private String getDefaultCacheResult(String name) {
      ++nbCall;
      return "Hi" +  name + "!";
   }


   public int getNbCall() {
      return nbCall;
   }
}
