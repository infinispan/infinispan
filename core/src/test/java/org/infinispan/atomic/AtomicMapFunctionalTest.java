package org.infinispan.atomic;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "atomic.AtomicMapFunctionalTest")
public class AtomicMapFunctionalTest extends SingleCacheManagerTest {
   private static final Log log = LogFactory.getLog(AtomicMapFunctionalTest.class);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = buildConfiguration();
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.invocationBatching().enable();
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
      return builder;
   }

   public void testAtomicMapWithoutTransactionManagerLookupSet() {
      ConfigurationBuilder builder = buildConfiguration();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(null);
      cacheManager.defineConfiguration("ahm_without_tmlookup", builder.build());
      Cache<String, String> ahmCache = cacheManager.getCache("ahm_without_tmlookup");

      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(ahmCache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(ahmCache, "key").get("a").equals("b");
   }

   public void testAtomicMapWithoutBatchSet() {
      ConfigurationBuilder builder = buildConfiguration();
      builder.invocationBatching().disable();
      cacheManager.defineConfiguration("ahm_without_batch", builder.build());
      Cache<String, String> ahmCache = cacheManager.getCache("ahm_without_batch");

      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(ahmCache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(ahmCache, "key").get("a").equals("b");
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testAtomicMapNonTransactionWithoutBatchSet() {
      ConfigurationBuilder builder = buildConfiguration();
      builder.invocationBatching().disable();
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      cacheManager.defineConfiguration("ahm_no_tx_without_batch", builder.build());
      Cache<String, String> ahmCache = cacheManager.getCache("ahm_no_tx_without_batch");

      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(ahmCache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(ahmCache, "key").get("a").equals("b");
   }

   public void testFineGrainedAtomicMapWithoutTransactionManagerLookupSet() {
      ConfigurationBuilder builder = buildConfiguration();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(null);
      cacheManager.defineConfiguration("fgahm_without_tmlookup", builder.build());
      Cache<String, String> fgahmCache = cacheManager.getCache("fgahm_without_tmlookup");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(fgahmCache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getFineGrainedAtomicMap(fgahmCache, "key").get("a").equals("b");
   }

   public void testFineGrainedAtomicMapWithoutBatchSet() {
      ConfigurationBuilder builder = buildConfiguration();
      builder.invocationBatching().disable();
      cacheManager.defineConfiguration("fgahm_without_batch", builder.build());
      Cache<String, String> fgahmCache = cacheManager.getCache("fgahm_without_batch");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(fgahmCache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getFineGrainedAtomicMap(fgahmCache, "key").get("a").equals("b");
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testFineGrainedAtomicMapNonTransactionWithoutBatchSet() {
      ConfigurationBuilder builder = buildConfiguration();
      builder.invocationBatching().disable();
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      cacheManager.defineConfiguration("fgahm_no_tx_without_batch", builder.build());
      Cache<String, String> fgahmCache = cacheManager.getCache("fgahm_no_tx_without_batch");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(fgahmCache, "key");
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getFineGrainedAtomicMap(fgahmCache, "key").get("a").equals("b");
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
      tm().begin();
      assert map.isEmpty();
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm().suspend();

      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a") == null;

      tm().resume(t);
      tm().commit();

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocks() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
//      InvocationContextContainer icc = TestingUtil.extractComponent(cache, InvocationContextContainer.class);
//      InvocationContext ic = icc.createInvocationContext(false, -1);
//      ic.setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
//      assert icc.getInvocationContext(true).hasFlag(SKIP_LOCKING);
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testTxChangesOnAtomicMapNoLocks() throws Exception {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      tm().begin();
      assert map.isEmpty();
//      TestingUtil.extractComponent(cache, InvocationContextContainer.class).createInvocationContext(true, -1).setFlags(SKIP_LOCKING);
      map.put("a", "b");
      assert map.get("a").equals("b");
      Transaction t = tm().suspend();

      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a") == null;

      tm().resume(t);
      tm().commit();

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   public void testChangesOnAtomicMapNoLocksExistingData() {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      assert map.isEmpty();
      map.put("x", "y");
      assert map.get("x").equals("y");
//      TestingUtil.extractComponent(cache, InvocationContextContainer.class).createInvocationContext(false, -1).setFlags(SKIP_LOCKING);
      log.debug("Doing a put");
      map.put("a", "b");
      log.debug("Put complete");
      assert map.get("a").equals("b");
      assert map.get("x").equals("y");

      // now re-retrieve the map and make sure we see the diffs
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("x").equals("y");
      assert AtomicMapLookup.getAtomicMap(cache, "key").get("a").equals("b");
   }

   @Test(expectedExceptions = IllegalStateException.class)
   public void testRemovalOfAtomicMap() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache, "key");
      map.put("hello", "world");
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      map = AtomicMapLookup.getAtomicMap(cache, "key");
      map.put("hello2", "world2");
      assert map.size() == 2;
      AtomicMapLookup.removeAtomicMap(cache, "key");
      map.size();
      tm.commit();
   }
}
