package org.infinispan.profiling;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Test class that verifies how quickly Cache instances are created under different scenarios
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "marshall.VersionAwareMarshallerTest")
public class CacheCreationStressTest extends AbstractInfinispanTest {

   public void testCreateCachesFromSameContainer() {
      long start = System.currentTimeMillis();
      CacheContainer container = new DefaultCacheManager();
      for (int i = 0; i < 1000; i++) {
         Cache cache = container.getCache(generateRandomString(20));
      }
      System.out.println("Took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
      TestingUtil.sleepThread(2000);
   }

   public static String generateRandomString(int numberOfChars) {
      Random r = new Random(System.currentTimeMillis());
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numberOfChars; i++) sb.append((char) (64 + r.nextInt(26)));
      return sb.toString();
   }

}
