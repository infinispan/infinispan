package org.infinispan.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Test the condition described here: {@link org.infinispan.interceptors.distribution.TxDistributionInterceptor#ignorePreviousValueOnBackup}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "api.ReplaceWithValueChangedTest")
public class ReplaceWithValueChangedTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true));
   }

   public void testReplace1() throws Throwable {
      Object k1 = getKeyForCache(0);
      cache(0).put(k1, "v1");
      tm(0).begin();
      assertEquals("v1", cache(0).replace(k1, "v2"));
      Transaction suspendedTx = tm(0).suspend();

      cache(0).remove(k1);
      assertNull(cache(0).get(k1));
      assertNull(cache(1).get(k1));

      log.trace("Here it begins");
      suspendedTx.commit();

      assertEquals("v2", cache(0).get(k1));
      assertEquals("v2", cache(1).get(k1));
   }

   public void testReplace2() throws Throwable {
      Object k1 = getKeyForCache(0);
      cache(0).put(k1, "v1");
      tm(0).begin();
      assertEquals("v1", cache(0).replace(k1, "v2"));
      Transaction suspendedTx = tm(0).suspend();

      cache(0).put(k1, "v3");
      assertEquals(cache(0).get(k1), "v3");
      assertEquals(cache(1).get(k1), "v3");

      suspendedTx.commit();

      assertEquals("v2", cache(0).get(k1));
      assertEquals("v2", cache(1).get(k1));
   }

   public void testPutIfAbsent() throws Throwable {
      Object k1 = getKeyForCache(0);

      tm(0).begin();
      assertNull(cache(0).putIfAbsent(k1, "v1"));
      Transaction suspendedTx = tm(0).suspend();

      cache(0).put(k1, "v2");
      assertEquals(cache(0).get(k1), "v2");
      assertEquals(cache(1).get(k1), "v2");

      suspendedTx.commit();

      assertEquals("v1", cache(0).get(k1));
      assertEquals("v1", cache(1).get(k1));
   }

   public void testConditionalRemove() throws Throwable {
      Object k1 = getKeyForCache(0);
      cache(0).put(k1, "v1");
      tm(0).begin();
      assertTrue(cache(0).remove(k1, "v1"));
      Transaction suspendedTx = tm(0).suspend();

      cache(0).put(k1, "v2");
      assertEquals(cache(0).get(k1), "v2");
      assertEquals(cache(1).get(k1), "v2");

      log.trace("here it is");
      suspendedTx.commit();

      assertNull(cache(0).get(k1));
      assertNull(cache(1).get(k1));
   }
}
