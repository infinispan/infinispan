package org.infinispan.stress;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.infinispan.Cache;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test  (groups = "stress", description = "designed to be run by hand", enabled = false)
public class MemoryCleanupTest {


   @BeforeTest
   public void createCm() {
   }

   public void testMemoryConsumption () throws InterruptedException {
      final ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(config);

      Cache<Object,Object> cache = cm.getCache();

      long freeMemBefore = freeMemKb();
      System.out.println("freeMemBefore = " + freeMemBefore);

      for (int i =0; i < 1024 * 300; i++) {
         cache.put(i,i);
      }
      System.out.println("Free meme after: " + freeMemKb());
      System.out.println("Consumed memory: " + (freeMemBefore - freeMemKb()));
      cm.stop();
      for (int i = 0; i<10; i++) {
         System.gc();
         if (isOkay(freeMemBefore)) {
            break;
         } else {
            Thread.sleep(1000);
         }
      }
      System.out.println("Free memory at the end:" + freeMemKb());
      assert isOkay(freeMemBefore);

   }

   private boolean isOkay(long freeMemBefore) {
      return freeMemBefore < freeMemKb() + 0.1 * freeMemKb();
   }


   public long freeMemKb() {
      return Runtime.getRuntime().freeMemory() / 1024;
   }
}
