package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContext;
import static org.infinispan.invocation.Flag.SKIP_LOCKING;
import org.infinispan.invocation.InvocationContextContainer;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.DummyTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "atomic.AtomicMapFunctionalTest")
public class AtomicMapFunctionalTest {
   private static final Log log = LogFactory.getLog(AtomicMapFunctionalTest.class);
   AtomicMapCache<String, String> cache;
   TransactionManager tm;

   @BeforeMethod
   @SuppressWarnings("unchecked")
   public void setUp() {
      Configuration c = new Configuration();
      // these 2 need to be set to use the AtomicMapCache
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      c.setInvocationBatchingEnabled(true);
      CacheManager cm = TestCacheManagerFactory.createCacheManager(c);
      Cache basicCache = cm.getCache();
      cache = (AtomicMapCache<String, String>) basicCache;
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCaches(cache);
   }

   public void testChangesOnAtomicMap() {
      AtomicMap<String, String> map = cache.getAtomicMap("key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert cache.getAtomicMap("key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMap() throws Exception {
      AtomicMap<String, String> map = cache.getAtomicMap("key");
      tm.begin();
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm.suspend();

      assert cache.getAtomicMap("key").get("a") == null;

      tm.resume(t);
      tm.commit();

      // now re-retrieve the map and make sure we see the diffs
      assert cache.getAtomicMap("key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocks() {
      AtomicMap<String, String> map = cache.getAtomicMap("key");
      assert map.isEmpty();
      InvocationContextContainer icc = TestingUtil.extractComponent(cache, InvocationContextContainer.class);
      InvocationContext ic = icc.get();
      ic.setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
      assert icc.get().hasFlag(SKIP_LOCKING);
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert cache.getAtomicMap("key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMapNoLocks() throws Exception {
      AtomicMap<String, String> map = cache.getAtomicMap("key");
      tm.begin();
      assert map.isEmpty();
      TestingUtil.extractComponent(cache, InvocationContextContainer.class).get().setFlags(SKIP_LOCKING);
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm.suspend();

      assert cache.getAtomicMap("key").get("a") == null;

      tm.resume(t);
      tm.commit();

      // now re-retrieve the map and make sure we see the diffs
      assert cache.getAtomicMap("key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocksExistingData() {
      AtomicMap<String, String> map = cache.getAtomicMap("key");
      assert map.isEmpty();
      map.put("x", "y");
      assert map.get("x").equals("y");
      TestingUtil.extractComponent(cache, InvocationContextContainer.class).get().setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");
      assert map.get("x").equals("y");

      // now re-retrieve the map and make sure we see the diffs
      assert cache.getAtomicMap("key").get("x").equals("y");
      assert cache.getAtomicMap("key").get("a").equals("b");
   }
}
