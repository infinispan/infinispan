package org.infinispan;

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
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Test(groups = "functional", testName = "marshall.VersionAwareMarshallerTest")
public class StressCacheCreationTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(StressCacheCreationTest.class);

   public void test000() {
      long start = System.currentTimeMillis();
      CacheContainer container = new DefaultCacheManager();
      for (int i = 0; i < 1000; i++) {
         container.getCache(generateRandomString(20));
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
