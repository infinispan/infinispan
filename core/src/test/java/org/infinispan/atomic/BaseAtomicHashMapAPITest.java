package org.infinispan.atomic;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

/**
 * Extracted originally from FineGrainedAtomicMapAPITest
 *
 * @author Vladimir Blagojevic (C) 2011 Red Hat Inc.
 * @author Sanne Grinovero (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class BaseAtomicHashMapAPITest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.clustering().hash().numSegments(60).groups().enabled();
      configurationBuilder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createClusteredCaches(2, "atomic", configurationBuilder);
   }

   public void testMultipleTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final TransactionManager tm1 = tm(cache1);

      final Map<String, String> map1 = createAtomicMap(cache1, "testMultipleTx", true);
      final Map<String, String> map2 = createAtomicMap(cache2, "testMultipleTx", false);

      Map<Object, Object> expectedMap = Collections.emptyMap();
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      tm1.begin();
      map1.put("k1", "initial");
      tm1.commit();

      expectedMap = createMap("k1", "initial");
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      tm1.begin();
      map1.put("k1", "v1");
      map1.put("k2", "v2");
      map1.put("k3", "v3");
      tm1.commit();

      expectedMap = createMap("k1", "v1", "k2", "v2", "k3", "v3");
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      tm1.begin();
      map1.put("k4", "v4");
      map1.put("k5", "v5");
      map1.put("k6", "v6");
      tm1.commit();

      expectedMap = createMap("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4", "k5", "v5", "k6", "v6");
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);
   }

   public void testSizeOnCache() throws Exception {
      final Cache<Object, Object> cache1 = cache(0, "atomic");
      final TransactionManager tm1 = tm(0, "atomic");
      assertSize(cache1, 0);
      cache1.put(new MagicKey("Hi", cache1), "Someone");
      assertSize(cache1, 1);

      tm1.begin();
      assertSize(cache1, 1);
      cache1.put(new MagicKey("Need", cache1), "Read Consistency");
      assertSize(cache1, 2);
      tm1.commit();
      assertSize(cache1, 2);

      tm1.begin();
      assertSize(cache1, 2);
      cache1.put(new MagicKey("Need Also", cache1), "Speed");
      assertSize(cache1, 3);
      tm1.rollback();
      assertSize(cache1, 2);

      Map<Object, Object> atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache", cache1), true);
      assertSize(cache1, 3);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 3);

      tm1.begin();
      assertSize(cache1, 3);
      atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache-second", cache1), true);
      assertSize(cache1, 4);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 4);
      tm1.commit();
      assertSize(cache1, 4);

      tm1.begin();
      assertSize(cache1, 4);
      atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache-third", cache1), true);
      assertSize(cache1, 5);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 5);
      atomicMap.put("ooo", "weird!");
      assertSize(cache1, 5);
      atomicMap = createAtomicMap(cache1, new MagicKey("testSizeOnCache-onemore", cache1), true);
      assertSize(cache1, 6);
      atomicMap.put("even less?", "weird!");
      assertSize(cache1, 6);
      tm1.rollback();
      assertSize(cache1, 4);
   }

   public void testConcurrentReadsOnExistingMap() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assertSize(cache1, 0);
      final Map<String, String> map = createAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
      map.put("the-1", "my preciousss");
      final Map<Object, Object> expectedMap = createMap("the-1", "my preciousss");

      tm(0, "atomic").begin();
      assertMap(expectedMap, map);
      map.put("the-2", "a minor");

      Future<Void> f = fork(TestingUtil.withTxCallable(tm(0, "atomic"), (Callable<Void>) () -> {
         Map<String, String> map1 = createAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
         assertMap(expectedMap, map1);
         assertNotContainsKey(map1, "the-2");
         return null;
      }));

      f.get(10, TimeUnit.SECONDS);

      tm(0, "atomic").commit();
   }

   public void testConcurrentWritesOnExistingMap() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assertSize(cache1, 0);
      final Map<String, String> map = createAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
      map.put("the-1", "my preciousss");
      final Map<Object, Object> expectedMap = createMap("the-1", "my preciousss");

      tm(0, "atomic").begin();
      assertMap(expectedMap, map);
      map.put("the-2", "a minor");

      Future<Void> f = fork(TestingUtil.withTxCallable(tm(0, "atomic"), (Callable<Void>) () -> {
         Map<String, String> map1 = createAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
         assertMap(expectedMap, map1);
         assertNotContainsKey(map1, "the-2");
         map1.put("the-2", "a minor-different"); // We're in pessimistic locking, so this put is going to block
         return null;
      }));

      try {
         f.get(10, TimeUnit.SECONDS);
         fail("We should have received a wrapped TimeoutException");
      } catch (ExecutionException e) {
         Throwable t = e;
         do {
            // We should have gotten a timeout exception since lock was held in other transaction
            if (t instanceof TimeoutException) {
               break;
            }
         } while ((t = t.getCause()) != null);
         assertNotNull("We should have found a TimeoutException!", t);
      }

      tm(0, "atomic").commit();
   }

   public void testConcurrentWritesAndIteration() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assertSize(cache1, 0);
      final Map<String, String> map = createAtomicMap(cache1, "testConcurrentWritesAndIteration", true);
      assertSize(map, 0);
      Callable<Void> c1 = () -> {
         Map<String, String> map1 = createAtomicMap(cache1, "testConcurrentWritesAndIteration", true);
         for (int i = 0; i < 250; i++) {
            map1.put("key-" + i, "value-" + i);
         }
         return null;
      };

      Callable<Void> c2 = () -> {
         Map<String, String> map12 = createAtomicMap(cache1, "testConcurrentWritesAndIteration", true);
         for (int i = 0; i < 250; i++) {
            map12.keySet();
         }
         return null;
      };
      CompletionService<Void> cs = runConcurrentlyWithCompletionService(c1, c2);

      for (int i = 0; i < 2; i++) {
         Future<Void> future = cs.poll(1, TimeUnit.MINUTES);
         // No timeout since it is guaranteed to be done
         if (future == null) {
            fail("Execution of forked tasks didn't complete within 1 minute");
         }
         future.get();
      }
   }

   public void testRollback() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      assertSize(cache1, 0);
      assertSize(cache2, 0);
      final Map<String, String> map1 = createAtomicMap(cache1, "testRollback", true);

      tm(0, "atomic").begin();
      map1.put("k1", "v");
      map1.put("k2", "v2");
      tm(0, "atomic").rollback();
      Map<Object, Object> instance = createAtomicMap(cache2, "testRollback", true);
      assertMap(Collections.emptyMap(), instance);
      assertMap(Collections.emptyMap(), map1);
   }

   public void testRollbackAndThenCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      assertSize(cache1, 0);
      assertSize(cache2, 0);
      final Map<String, String> map1 = createAtomicMap(cache1, "testRollbackAndThenCommit", true);

      tm(0, "atomic").begin();
      map1.put("k1", "v");
      map1.put("k2", "v2");
      tm(0, "atomic").rollback();
      Map<Object, Object> expectedMap = Collections.emptyMap();

      Map<Object, Object> map2 = createAtomicMap(cache2, "testRollbackAndThenCommit", true);
      assertMap(expectedMap, map2);
      assertMap(expectedMap, map1);

      tm(0, "atomic").begin();
      map1.put("k3", "v3");
      map1.put("k4", "v4");
      tm(0, "atomic").commit();

      expectedMap = createMap("k3", "v3", "k4", "v4");
      assertMap(expectedMap, map2);
      assertMap(expectedMap, map1);
   }

   public void testCreateMapInTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      tm(0, "atomic").begin();
      Map<String, String> map1 = createAtomicMap(cache1, "testCreateMapInTx", true);
      map1.put("k1", "v1");
      tm(0, "atomic").commit();

      Map<Object, Object> expectedMap = createMap("k1", "v1");
      assertMap(expectedMap, map1);

      final Map<String, String> map2 = createAtomicMap(cache2, "testCreateMapInTx", true);
      assertMap(expectedMap, map2);
   }

   @SuppressWarnings("UnusedDeclaration")
   public void testNoTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final Map<String, String> map = createAtomicMap(cache1, "testNoTx", true);
      map.put("existing", "existing");
      map.put("blah", "blah");

      Map<Object, Object> expectedMap = createMap("existing", "existing", "blah", "blah");
      assertMap(expectedMap, map);
   }

   @SuppressWarnings("UnusedDeclaration")
   public void testReadUncommittedValues() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      Map<String, String> map = createAtomicMap(cache1, "testReadUncommittedValues");

      tm(cache1).begin();
      map.put("key one", "value one");
      map.put("blah", "blah");

      Map<Object, Object> expectedMap = createMap("key one", "value one", "blah", "blah");

      assertMap(expectedMap, map);

      Map<String, String> sameAsMap = createAtomicMap(cache1, "testReadUncommittedValues");
      assertMap(expectedMap, sameAsMap);
      tm(cache1).commit();

      assertMap(expectedMap, map);
      assertMap(expectedMap, sameAsMap);

      expectedMap = createMap("blah", "blah");
      //now remove one of the elements in a transaction:
      tm(cache1).begin();
      map = createAtomicMap(cache1, "testReadUncommittedValues");
      String removed = map.remove("key one");
      assertEquals("Wrong value removed.", "value one", removed);
      assertNotContainsKey(map, "key one");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);
      tm(cache1).commit();

      //verify state after commit:
      map = createAtomicMap(cache1, "testReadUncommittedValues");
      removed = map.remove("key one");
      assertNull("Wrong value removed.", removed);
      assertNotContainsKey(map, "key one");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);

      expectedMap = createMap("key one", "value one", "blah", "blah");

      //add the removed element back:
      tm(cache1).begin();
      map = createAtomicMap(cache1, "testReadUncommittedValues");
      map.put("key one", "value one");
      tm(cache1).commit();
      assertMap(expectedMap, map);

      expectedMap = createMap("key one", "value two", "blah", "blah");

      //now test for element replacement:
      tm(cache1).begin();
      map = createAtomicMap(cache1, "testReadUncommittedValues");
      map.put("key one", "value two");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);
      tm(cache1).commit();

      //verify state after commit:
      map = createAtomicMap(cache1, "testReadUncommittedValues");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);
   }

   @SuppressWarnings("UnusedDeclaration")
   public void testCommitReadUncommittedValues() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final Map<String, String> map = createAtomicMap(cache1, "testCommitReadUncommittedValues");
      tm(cache1).begin();
      map.put("existing", "existing");
      map.put("hey", "blah");
      tm(cache1).commit();

      tm(cache1).begin();
      map.put("key one", "fake one");
      map.put("key one", "value one");
      map.put("blah", "montevideo");
      map.put("blah", "buenos aires");
      map.remove("blah");
      map.put("blah", "toronto");

      Map<Object, Object> expectedMap = createMap("key one", "value one", "blah", "toronto", "existing", "existing", "hey", "blah");
      assertMap(expectedMap, map);

      Map<String, String> sameAsMap = createAtomicMap(cache1, "testCommitReadUncommittedValues");
      assertMap(expectedMap, sameAsMap);
      tm(cache1).commit();

      assertMap(expectedMap, map);
      assertMap(expectedMap, sameAsMap);
   }

   public void testConcurrentTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final TransactionManager tm1 = tm(cache1);
      final TransactionManager tm2 = tm(cache2);

      final Map<String, String> map1 = createAtomicMap(cache1, "testConcurrentTx", true);
      Map<Object, Object> expectedMap = createMap("k1", "initial");

      tm1.begin();
      map1.put("k1", "initial");
      tm1.commit();

      final Map<String, String> map2 = createAtomicMap(cache2, "testConcurrentTx", false);

      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      Future<Void> t1 = fork(TestingUtil.withTxCallable(tm1, (Callable<Void>) () -> {
         map1.put("k1", "tx1Value");
         return null;
      }));

      Future<Void> t2 = fork(TestingUtil.withTxCallable(tm2, (Callable<Void>) () -> {
         map2.put("k2", "tx2Value");
         return null;
      }));

      t2.get();
      t1.get();

      expectedMap = createMap("k1", "tx1Value", "k2", "tx2Value");
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);
   }

   public void testReplicationPutCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final Map<String, String> map = createAtomicMap(cache1, "testReplicationPutCommit");
      Map<Object, Object> expectedMap = createMap("existing", "existing", "blah", "blah");

      tm(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      tm(cache1).commit();

      assertMap(expectedMap, map);

      final Map<Object, Object> other = createAtomicMap(cache2, "testReplicationPutCommit", false);
      assertMap(expectedMap, other);

      //ok, do another tx with delta changes
      tm(cache2).begin();
      other.put("existing", "not existing");
      other.put("not existing", "peace on Earth");
      tm(cache2).commit();

      expectedMap = createMap("blah", "blah", "existing", "not existing", "not existing", "peace on Earth");
      assertMap(expectedMap, map);
      assertMap(expectedMap, other);
   }

   public void testReplicationRemoveCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final Map<String, String> map = createAtomicMap(cache1, "testReplicationRemoveCommit");

      Map<Object, Object> expectedMap = createMap("existing", "existing", "blah", "blah");

      tm(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      tm(cache1).commit();

      assertMap(expectedMap, map);

      final Map<Object, Object> other = createAtomicMap(cache2, "testReplicationRemoveCommit", false);
      assertMap(expectedMap, other);

      //ok, do another tx with delta changes
      tm(cache2).begin();
      String removed = map.remove("existing");
      assertEquals("Wrong value removed from map '" + map + "'.", "existing", removed);
      tm(cache2).commit();

      expectedMap = createMap("blah", "blah");
      assertMap(expectedMap, map);
      assertMap(expectedMap, other);
   }

   public void testReplicationPutAndClearCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      final Map<String, String> map = createAtomicMap(cache1, "map");
      final Map<String, String> map2 = createAtomicMap(cache2, "map", false);
      Map<Object, Object> expectedMap = createMap("existing", "existing", "blah", "blah");

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      map.size();
      TestingUtil.getTransactionManager(cache1).commit();

      assertMap(expectedMap, map);
      assertMap(expectedMap, map2);

      //ok, do another tx with clear delta changes
      tm(cache2).begin();
      map2.clear();
      tm(cache2).commit();

      expectedMap = Collections.emptyMap();
      assertMap(expectedMap, map);
      assertMap(expectedMap, map2);
   }

   public void testInsertDeleteInsertCycle() throws Exception {
      final Cache<MagicKey, Object> cache2 = cache(1, "atomic");
      testInsertDeleteCycle(new MagicKey(cache2));
   }

   public void testInsertDeleteInsertCyclePrimaryOwner() throws Exception {
      final Cache<MagicKey, Object> cache1 = cache(0, "atomic");
      testInsertDeleteCycle(new MagicKey(cache1));
   }

   private void testInsertDeleteCycle(MagicKey key) throws Exception {
      final Cache<MagicKey, Object> cache1 = cache(0, "atomic");
      final Cache<MagicKey, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      {
         TestingUtil.getTransactionManager(cache1).begin();
         Map<String, Object> am = createAtomicMap(cache1, key);
         am.put("k1", "v1");
         TestingUtil.getTransactionManager(cache1).commit();
      }
      {
         TestingUtil.getTransactionManager(cache1).begin();
         Map<String, Object> am = createAtomicMap(cache1, key);
         am.put("k3", "v3");
         removeAtomicMap(cache1, key); //!
         am = createAtomicMap(cache1, key);
         am.put("k2", "v2");
         TestingUtil.getTransactionManager(cache1).commit();
      }
      final boolean fails;
      {
         TestingUtil.getTransactionManager(cache1).begin();
         Map<String, Object> am = createAtomicMap(cache1, key);
         //'k1' should no longer exist as we had killed the whole am instance in previous transaction
         fails = am.containsKey("k1");
         TestingUtil.getTransactionManager(cache1).commit();
      }
      assertFalse(fails);
   }

   public void testDuplicateValue() {
      final Cache<String, Object> cache = cache(0, "atomic");
      assertSize(cache, 0);
      final Map<String, String> map = createAtomicMap(cache, "duplicateValues", true);

      map.put("k1", "value");
      map.put("k2", "value");
      map.put("k3", "value");
      map.put("k4", "value");

      final Map<Object, Object> expectedMap = createMap("k1", "value", "k2", "value", "k3", "value", "k4", "value");
      assertMap(expectedMap, map);
   }

   public void testReadEntriesCommittedInConcurrentTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      assertSize(cache1, 0);
      assertSize(cache2, 0);

      if (cache1.getCacheConfiguration().locking().isolationLevel() != IsolationLevel.REPEATABLE_READ ||
            cache2.getCacheConfiguration().locking().isolationLevel() != IsolationLevel.REPEATABLE_READ) {
         //this test only makes sense with Repeatable Read!
         return;
      }

      final TransactionManager tm1 = tm(cache1);
      final TransactionManager tm2 = tm(cache2);

      final Map<String, String> writeMap = createAtomicMap(cache1, "repeatableReadMap", true);


      Map<Object, Object> emptyMap = Collections.emptyMap();
      assertMap(emptyMap, writeMap);

      // tx1
      tm1.begin();
      writeMap.put("k1", "initial");
      tm1.commit();

      Map<Object, Object> initialMap = createMap("k1", "initial");
      assertMap(initialMap, writeMap);

      // tx2
      tm2.begin();
      Map<String, String> readMap = createAtomicMap(cache2, "repeatableReadMap", false);
      assertMap(initialMap, readMap);
      Transaction tx2 = tm2.suspend();

      // tx3
      tm1.begin();
      writeMap.put("k1", "v1");
      writeMap.put("k2", "v2");
      writeMap.put("k3", "v3");

      Map<Object, Object> modifiedMap = createMap("k1", "v1", "k2", "v2", "k3", "v3");
      assertMap(modifiedMap, writeMap);
      tm1.commit();

      // tx2
      tm2.resume(tx2);
      // a new FGAM referring to the same key should not see the entries committed by tx3
      assertMap(initialMap, readMap);
      tm2.commit();

      // tx4
      assertMap(modifiedMap, writeMap);
      assertMap(modifiedMap, readMap);
   }

   protected void assertSize(Map<?, ?> map, int expectedSize) {
      final int size;
      if (map instanceof Cache) {
         size = ((Cache)map).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).size();
      } else {
         size = map.size();
      }
      assertEquals("Wrong size in map '" + map + "'.", expectedSize, size);
      if (size == 0) {
         assertEmpty(map);
      } else {
         assertNotEmpty(map);
      }
   }

   private void assertEmpty(Map<?, ?> map) {
      assertTrue("Map '" + map + "' should be empty.", map.isEmpty());
   }

   private void assertNotEmpty(Map<?, ?> map) {
      assertFalse("Map '" + map + "' should *not* be empty.", map.isEmpty());
   }

   private void assertKeyValue(Map<?, ?> map, Object key, Object expectedValue) {
      assertEquals("Wrong value for key '" + key + "' on map '" + map + "'.", expectedValue, map.get(key));
   }

   private void assertKeysValues(Map<?, ?> expected, Map<?, ?> atomicMap) {
      for (Entry<?, ?> entry : expected.entrySet()) {
         assertKeyValue(atomicMap, entry.getKey(), entry.getValue());
      }
   }

   private void assertNotContainsKey(Map<?, ?> map, Object key) {
      assertFalse("Map '" + map + "' should *not* contain key '" + key + "'.", map.containsKey(key));
   }

   private void assertContainsKey(Map<?, ?> map, Object key) {
      assertTrue("Map '" + map + "' should contain key '" + key + "'.", map.containsKey(key));
   }

   private void assertNotContainsValue(Map<?, ?> map, Object value) {
      assertFalse("Map '" + map + "' should *not* contain value '" + value + "'.", map.containsValue(value));
   }

   private void assertContainsValue(Map<?, ?> map, Object value) {
      assertTrue("Map '" + map + "' should contain value '" + value + "'.", map.containsValue(value));
   }

   protected void assertMap(Map<?, ?> expected, Map<?, ?> atomicMap) {
      // do a simple equals() check first
      assertEquals(expected, new HashMap<Object, Object>(atomicMap));

      //check size() and isEmpty()
      assertEquals("Wrong map '" + atomicMap + "'.size()", expected.size(), atomicMap.size());
      assertEquals("Wrong map '" + atomicMap + "'.isEmpty()", expected.isEmpty(), atomicMap.isEmpty());
      assertEquals("Wrong map '" + atomicMap + "'.keySet().size()", expected.keySet().size(), atomicMap.keySet().size());
      assertEquals("Wrong map '" + atomicMap + "'.keySet().isEmpty()", expected.keySet().isEmpty(), atomicMap.keySet().isEmpty());
      assertEquals("Wrong map '" + atomicMap + "'.values().size()", expected.values().size(), atomicMap.values().size());
      assertEquals("Wrong map '" + atomicMap + "'.values().isEmpty()", expected.values().isEmpty(), atomicMap.values().isEmpty());
      assertEquals("Wrong map '" + atomicMap + "'.entrySet().size()", expected.entrySet().size(), atomicMap.entrySet().size());
      assertEquals("Wrong map '" + atomicMap + "'.entrySet().isEmpty()", expected.entrySet().isEmpty(), atomicMap.entrySet().isEmpty());

      //check the collections content
      assertEquals("Wrong map '" + atomicMap + "'.keySet() content.", expected.keySet(), atomicMap.keySet());
      assertEquals("Wrong map '" + atomicMap + "'.values() content.", new HashSet<Object>(expected.values()), new HashSet<Object>(atomicMap.values()));
      for (Entry<?, ?> entry : atomicMap.entrySet()) {
         assertEquals("Wrong value for key " + entry.getKey(), expected.get(entry.getKey()), entry.getValue());
      }

      //check the containsKey()
      for (Object key : expected.keySet()) {
         assertContainsKey(atomicMap, key);
      }

      //check the containsValue()
      for (Object value : expected.values()) {
         assertContainsValue(atomicMap, value);
      }

      //check the get()
      assertKeysValues(expected, atomicMap);
   }

   /**
    * @param keysAndValues Alternating keys and values.
    */
   protected Map<Object, Object> createMap(Object... keysAndValues) {
      assertEquals("Wrong parameters in createMap() method.", 0, keysAndValues.length % 2);
      Map<Object, Object> map = new HashMap<>();
      for (int i = 0; i < keysAndValues.length; i += 2) {
         map.put(keysAndValues[i], keysAndValues[i + 1]);
      }
      return map;
   }

   protected abstract <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key, boolean createIfAbsent);

   protected final <CK, K, V> Map<K, V> createAtomicMap(Cache<CK, Object> cache, CK key) {
      return createAtomicMap(cache, key, true);
   }

   protected <CK> void removeAtomicMap(Cache<CK, Object> cache, CK key) {
      AtomicMapLookup.removeAtomicMap(cache, key);
   }

}
