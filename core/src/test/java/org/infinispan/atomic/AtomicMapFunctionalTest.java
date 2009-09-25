package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import static org.infinispan.context.Flag.SKIP_LOCKING;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "atomic.AtomicMapFunctionalTest")
public class AtomicMapFunctionalTest {
   private static final Log log = LogFactory.getLog(AtomicMapFunctionalTest.class);
   Cache<String, Object> cache;
   TransactionManager tm;

   @BeforeMethod
   @SuppressWarnings("unchecked")
   public void setUp() {
      Configuration c = new Configuration();
      // these 2 need to be set to use the AtomicMapCache
      c.setInvocationBatchingEnabled(true);
      CacheManager cm = TestCacheManagerFactory.createCacheManager(c, true);
      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCaches(cache);
   }

   public void testChangesOnAtomicMap() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMap() throws Exception {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      tm.begin();
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm.suspend();

      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a") == null;

      tm.resume(t);
      tm.commit();

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocks() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
      InvocationContextContainer icc = TestingUtil.extractComponent(cache, InvocationContextContainer.class);
      InvocationContext ic = icc.createInvocationContext();
      ic.setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
      assert icc.getInvocationContext().hasFlag(SKIP_LOCKING);
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMapNoLocks() throws Exception {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      tm.begin();
      assert map.isEmpty();
      TestingUtil.extractComponent(cache, InvocationContextContainer.class).createInvocationContext().setFlags(SKIP_LOCKING);
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm.suspend();

      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a") == null;

      tm.resume(t);
      tm.commit();

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocksExistingData() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
      map.put("x", "y");
      assert map.get("x").equals("y");
      TestingUtil.extractComponent(cache, InvocationContextContainer.class).createInvocationContext().setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");
      assert map.get("x").equals("y");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("x").equals("y");
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }
}
