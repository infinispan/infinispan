package org.infinispan.profiling;

import static org.infinispan.test.TestingUtil.withCacheManager;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test class that verifies how quickly Cache instances are created under different scenarios
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "profiling", testName = "profiling.CacheCreationStressTest")
public class CacheCreationStressTest extends AbstractInfinispanTest {

   public void testCreateCachesFromSameContainer() {
      final long start = System.currentTimeMillis();
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.createCacheManager()) {
         @Override
         public void call() {
            for (int i = 0; i < 1000; i++) {
               cm.getCache(generateRandomString(20));
            }
            System.out.println("Took: " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start));
            TestingUtil.sleepThread(2000);
         }
      });
   }

   public static String generateRandomString(int numberOfChars) {
      Random r = new Random(System.currentTimeMillis());
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numberOfChars; i++) sb.append((char) (64 + r.nextInt(26)));
      return sb.toString();
   }

}
