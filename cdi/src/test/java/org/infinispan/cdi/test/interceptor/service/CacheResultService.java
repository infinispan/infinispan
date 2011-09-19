package org.infinispan.cdi.test.interceptor.service;

import javax.cache.interceptor.CacheResult;
import javax.enterprise.context.ApplicationScoped;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@ApplicationScoped
public class CacheResultService {

   private int cacheResult;
   private int cacheResultWithCacheKeyGenerator;
   private int cacheResultSkipGet;
   private int cacheResultWithCacheName;
   private int cacheResultWithSpecificCacheManager;

   public CacheResultService() {
      this.cacheResult = 0;
      this.cacheResultWithCacheKeyGenerator = 0;
      this.cacheResultSkipGet = 0;
      this.cacheResultWithCacheName = 0;
      this.cacheResultWithSpecificCacheManager = 0;
   }

   @CacheResult
   public String cacheResult(String user) {
      cacheResult++;
      return "Morning " + user;
   }

   @CacheResult(cacheName = "custom")
   public String cacheResultWithCacheName(String user) {
      cacheResultWithCacheName++;
      return "Hi " + user;
   }

   @CacheResult(cacheName = "custom", cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public String cacheResultWithCacheKeyGenerator(String user) {
      cacheResultWithCacheKeyGenerator++;
      return "Hello " + user;
   }

   @CacheResult(cacheName = "custom", skipGet = true)
   public String cacheResultSkipGet(String user) {
      cacheResultSkipGet++;
      return "Hey " + user;
   }

   @CacheResult(cacheName = "small")
   public String cacheResultWithSpecificCacheManager(String user) {
      cacheResultWithSpecificCacheManager++;
      return "Bonjour " + user;
   }

   public int getCacheResultWithCacheKeyGenerator() {
      return cacheResultWithCacheKeyGenerator;
   }

   public int getCacheResultSkipGet() {
      return cacheResultSkipGet;
   }

   public int getCacheResultWithCacheName() {
      return cacheResultWithCacheName;
   }

   public int getCacheResult() {
      return cacheResult;
   }

   public int getCacheResultWithSpecificCacheManager() {
      return cacheResultWithSpecificCacheManager;
   }
}
