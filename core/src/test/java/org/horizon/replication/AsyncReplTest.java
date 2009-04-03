/*
 *
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.horizon.replication;

import org.horizon.Cache;
import org.horizon.config.Configuration;
import org.horizon.test.MultipleCacheManagersTest;
import org.horizon.test.TestingUtil;
import org.horizon.transaction.DummyTransactionManagerLookup;
import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Test(groups = "functional", testName = "replication.AsyncReplTest")
public class AsyncReplTest extends MultipleCacheManagersTest {

   Cache cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      Configuration asyncConfiguration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC);
      asyncConfiguration.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      List<Cache> caches = createClusteredCaches(2, "asyncRepl", asyncConfiguration);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

   public void testWithNoTx() throws Exception {

      String key = "key";

      replListener(cache2).expectAny();
      cache1.put(key, "value1");
      // allow for replication
      replListener(cache2).waitForRPC(60, TimeUnit.SECONDS);
      assertEquals("value1", cache1.get(key));
      assertEquals("value1", cache2.get(key));

      replListener(cache2).expectAny();
      cache1.put(key, "value2");
      assertEquals("value2", cache1.get(key));

      replListener(cache2).waitForRPC(60, TimeUnit.SECONDS);

      assertEquals("value2", cache1.get(key));
      assertEquals("value2", cache2.get(key));
   }

   public void testWithTx() throws Exception {
      String key = "key";
      replListener(cache2).expectAny();
      cache1.put(key, "value1");
      // allow for replication
      replListener(cache2).waitForRPC(60, TimeUnit.SECONDS);
      assertEquals("value1", cache1.get(key));
      assertEquals("value1", cache2.get(key));

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      replListener(cache2).expectAnyWithTx();
      cache1.put(key, "value2");
      assertEquals("value2", cache1.get(key));
      assertEquals("value1", cache2.get(key));

      mgr.commit();

      replListener(cache2).waitForRPC(60, TimeUnit.SECONDS);

      assertEquals("value2", cache1.get(key));
      assertEquals("value2", cache2.get(key));

      mgr.begin();
      cache1.put(key, "value3");
      assertEquals("value3", cache1.get(key));
      assertEquals("value2", cache2.get(key));

      mgr.rollback();

      assertEquals("value2", cache1.get(key));
      assertEquals("value2", cache2.get(key));
   }
}
