package org.infinispan.cdi.test.interceptor.service;

import javax.cache.interceptor.CacheResult;
import javax.enterprise.context.ApplicationScoped;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
@ApplicationScoped
public class GreetingService {

   private int sayMorningCount;
   private int sayHelloCount;
   private int sayHeyCount;
   private int sayHiCount;
   private int sayBonjourCount;

   public GreetingService() {
      this.sayMorningCount = 0;
      this.sayHelloCount = 0;
      this.sayHeyCount = 0;
      this.sayHiCount = 0;
      this.sayBonjourCount = 0;
   }

   @CacheResult
   public String sayMorning(String user) {
      sayMorningCount++;
      return "Morning " + user;
   }

   @CacheResult(cacheKeyGenerator = CustomCacheKeyGenerator.class)
   public String sayHello(String user) {
      sayHelloCount++;
      return "Hello " + user;
   }

   @CacheResult(skipGet = true)
   public String sayHey(String user) {
      sayHeyCount++;
      return "Hey " + user;
   }

   @CacheResult(cacheName = "custom")
   public String sayHi(String user) {
      sayHiCount++;
      return "Hi " + user;
   }

   @CacheResult(cacheName = "small")
   public String sayBonjour(String user) {
      sayBonjourCount++;
      return "Bonjour " + user;
   }

   public int getSayHelloCount() {
      return sayHelloCount;
   }

   public int getSayHeyCount() {
      return sayHeyCount;
   }

   public int getSayHiCount() {
      return sayHiCount;
   }

   public int getSayMorningCount() {
      return sayMorningCount;
   }

   public int getSayBonjourCount() {
      return sayBonjourCount;
   }
}
