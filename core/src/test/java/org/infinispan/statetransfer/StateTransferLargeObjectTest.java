package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-654.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional" , testName="statetransfer.StateTransferLargeObjectTest")
public class StateTransferLargeObjectTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(StateTransferLargeObjectTest.class);

   private Cache<Integer, BigObject> c0;
   private Cache<Integer, BigObject> c1;
   private Cache<Integer, BigObject> c2;
   private Cache<Integer, BigObject> c3;
   private Map<Integer, BigObject> cache;

   private final Random rnd = new Random();

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .l1().disable()
            .clustering().stateTransfer().fetchInMemoryState(true)
            .locking().useLockStriping(false)
            .clustering().hash().numOwners(3).numSegments(60);
      createCluster(builder, 4);

      c0 = cache(0);
      c1 = cache(1);
      c2 = cache(2);
      c3 = cache(3);
      waitForClusterToForm();
      log.debug("Rehash is complete!");
      cache = new HashMap<Integer, BigObject>();
   }

   public void testForFailure() {
      final int num = 1000;
      for (int i = 0; i < num; i++) {
         BigObject bigObject = createBigObject(i, "prefix");
         cache.put(i, bigObject);
         c0.put(i, bigObject);
      }

      for (int i = 0; i < num; i++) {
         assertTrue(c0.get(i) instanceof BigObject);
         assertTrue(c1.get(i) instanceof BigObject);
         assertTrue(c2.get(i) instanceof BigObject);
         assertTrue(c3.get(i) instanceof BigObject);
      }

      log.info("Before stopping a cache!");
      fork(new Runnable() {
         @Override
         public void run() {
            log.debug("About to stop " + c3.getAdvancedCache().getRpcManager().getAddress());
            c3.stop();
            c3.getCacheManager().stop();
            log.debug("Cache stopped async!");
         }
      });

      for (int i = 0; i < num; i++) {
         log.debug("----Running a get on " + i);
         assertValue(i, c0.get(i));
         assertValue(i, c1.get(i));
         assertValue(i, c2.get(i));
      }
      log.debug("Before stopping cache managers!");
      TestingUtil.killCacheManagers(manager(2));
      log.debug("2 killed");
      TestingUtil.killCacheManagers(manager(1));
      log.debug("1 killed");
      TestingUtil.killCacheManagers(manager(0));
      log.debug("0 killed");
   }

   private void assertValue(int i, Object o) {
      assertNotNull(o);
      assertTrue(o instanceof BigObject);
      assertEquals(o, cache.get(i));
   }

   private BigObject createBigObject(int num, String prefix) {
      BigObject obj = new BigObject();
      obj.setName("[" + num + "|" + prefix + "|" +  (num*3) + "|" + (num*7) + "]");
      obj.setValue(generateLargeString());
      return obj;
   }

   private String generateLargeString() {
      byte[] bytes = new byte[20 * 100];
      rnd.nextBytes(bytes);
      return new String(bytes);
   }

   @AfterMethod
   @Override
   protected void clearContent() {
   }
}
