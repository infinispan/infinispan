package org.infinispan.atomic;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.infinispan.atomic.AtomicMapLookup.getAtomicMap;
import static org.infinispan.atomic.AtomicMapLookup.getFineGrainedAtomicMap;
import static org.testng.AssertJUnit.*;

/**
 * @author Vladimir Blagojevic (C) 2011 Red Hat Inc.
 * @author Sanne Grinovero (C) 2011 Red Hat Inc.
 * @author Pedro Ruivo
 */
@Test(groups = "functional", testName = "atomic.FineGrainedAtomicMapAPITest")
public class FineGrainedAtomicMapAPITest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.transaction()
                  .transactionMode(TransactionMode.TRANSACTIONAL)
                  .lockingMode(LockingMode.PESSIMISTIC)
                  .locking().lockAcquisitionTimeout(100l);
      createCluster(configurationBuilder, 2);
   }

   public void testMultipleTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final TransactionManager tm1 = tm(cache1);

      final FineGrainedAtomicMap<String, String> map1 = getFineGrainedAtomicMap(cache1, "testMultipleTx", true);
      final FineGrainedAtomicMap<String, String> map2 = getFineGrainedAtomicMap(cache2, "testMultipleTx", false);

      Map<Object, Object> expectedMap = Collections.emptyMap();
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      tm1.begin();
      map1.put("k1", "initial");
      tm1.commit();

      expectedMap = createMap(new Object[]{"k1"}, new Object[]{"initial"});
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      tm1.begin();
      map1.put("k1", "v1");
      map1.put("k2", "v2");
      map1.put("k3", "v3");
      tm1.commit();

      expectedMap = createMap(new Object[]{"k1", "k2", "k3"},
                              new Object[]{"v1", "v2", "v3"});
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      tm1.begin();
      map1.put("k4", "v4");
      map1.put("k5", "v5");
      map1.put("k6", "v6");
      tm1.commit();

      expectedMap = createMap(new Object[]{"k1", "k2", "k3", "k4", "k5", "k6"},
                              new Object[]{"v1", "v2", "v3", "v4", "v5", "v6"});
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

      FineGrainedAtomicMap<Object, Object> atomicMap = getFineGrainedAtomicMap(cache1,
                                                                               new MagicKey("testSizeOnCache", cache1),
                                                                               true);
      assertSize(cache1, 3);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 3);

      tm1.begin();
      assertSize(cache1, 3);
      atomicMap = getFineGrainedAtomicMap(cache1, new MagicKey("testSizeOnCache-second", cache1), true);
      assertSize(cache1, 4);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 4);
      tm1.commit();
      assertSize(cache1, 4);

      tm1.begin();
      assertSize(cache1, 4);
      atomicMap = getFineGrainedAtomicMap(cache1, new MagicKey("testSizeOnCache-third", cache1), true);
      assertSize(cache1, 5);
      atomicMap.put("mm", "nn");
      assertSize(cache1, 5);
      atomicMap.put("ooo", "weird!");
      assertSize(cache1, 5);
      atomicMap = getFineGrainedAtomicMap(cache1, new MagicKey("testSizeOnCache-onemore", cache1), true);
      assertSize(cache1, 6);
      atomicMap.put("even less?", "weird!");
      assertSize(cache1, 6);
      tm1.rollback();
      assertSize(cache1, 4);
   }

   public void testConcurrentReadsOnExistingMap() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assertSize(cache1, 0);
      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                               "testConcurrentReadsOnExistingMap", true);
      map.put("the-1", "my preciousss");
      final Map<Object, Object> expectedMap = createMap(new Object[]{"the-1"}, new Object[]{"my preciousss"});

      tm(0, "atomic").begin();
      assertMap(expectedMap, map);
      final AtomicBoolean allOk = new AtomicBoolean(false);
      map.put("the-2", "a minor");

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(0, "atomic").begin();
               FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                                  "testConcurrentReadsOnExistingMap", true);
               assertMap(expectedMap, map);
               assertNotContainsKey(map, "the-2");
               tm(0, "atomic").commit();
               allOk.set(true);
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, true);

      tm(0, "atomic").commit();
      assertTrue(allOk.get());
   }

   public void testConcurrentWritesOnExistingMap() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assertSize(cache1, 0);
      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                               "testConcurrentReadsOnExistingMap", true);
      map.put("the-1", "my preciousss");
      final Map<Object, Object> expectedMap = createMap(new Object[]{"the-1"}, new Object[]{"my preciousss"});

      tm(0, "atomic").begin();
      assertMap(expectedMap, map);
      final AtomicBoolean allOk = new AtomicBoolean(false);
      map.put("the-2", "a minor");

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(0, "atomic").begin();
               FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                                  "testConcurrentReadsOnExistingMap", true);
               assertMap(expectedMap, map);
               assertNotContainsKey(map, "the-2");
               map.put("the-2", "a minor-different"); // We're in pessimistic locking, so this put is going to block
               tm(0, "atomic").commit();
            } catch (org.infinispan.util.concurrent.TimeoutException e) {
               allOk.set(true);
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, true);

      tm(0, "atomic").commit();
      assertTrue(allOk.get());
   }

   public void testConcurrentWritesAndIteration() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assertSize(cache1, 0);
      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                               "testConcurrentWritesAndIteration", true);
      assertSize(map, 0);
      final AtomicBoolean allOk = new AtomicBoolean(true);
      final CountDownLatch latch = new CountDownLatch(1);
      Thread t1 = fork(new Runnable() {
         @Override
         public void run() {
            try {
               FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                                  "testConcurrentWritesAndIteration", true);
               latch.await();
               for (int i = 0; i < 500; i++) {
                  map.put("key-" + i, "value-" + i);
               }
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, false);

      Thread t2 = fork(new Runnable() {
         @Override
         public void run() {
            FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1,
                                                                               "testConcurrentWritesAndIteration", true);
            try {
               latch.await();
               for (int i = 0; i < 500; i++) {
                  map.keySet();
               }
            } catch (Exception e) {
               allOk.set(false);
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, false);
      latch.countDown();
      t1.join();
      t2.join();
      assertTrue("Iteration raised an expection.", allOk.get());
   }

   public void testRollback() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      final FineGrainedAtomicMap<String, String> map1 = getFineGrainedAtomicMap(cache1, "testRollback", true);

      tm(0, "atomic").begin();
      map1.put("k1", "v");
      map1.put("k2", "v2");
      tm(0, "atomic").rollback();
      FineGrainedAtomicMap<Object, Object> instance = getFineGrainedAtomicMap(cache2, "testRollback", true);
      assertMap(Collections.emptyMap(), instance);
      assertMap(Collections.emptyMap(), map1);
   }

   @SuppressWarnings("UnusedDeclaration")
   @Test(expectedExceptions = {IllegalArgumentException.class})
   public void testFineGrainedMapAfterSimpleMap() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");

      AtomicMap<String, String> map = getAtomicMap(cache1, "testReplicationRemoveCommit");
      FineGrainedAtomicMap<String, String> map2 = getFineGrainedAtomicMap(cache1, "testReplicationRemoveCommit");
   }

   public void testRollbackAndThenCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      final FineGrainedAtomicMap<String, String> map1 = getFineGrainedAtomicMap(cache1,
                                                                                "testRollbackAndThenCommit", true);

      tm(0, "atomic").begin();
      map1.put("k1", "v");
      map1.put("k2", "v2");
      tm(0, "atomic").rollback();
      Map<Object, Object> expectedMap = Collections.emptyMap();

      FineGrainedAtomicMap<Object, Object> map2 = getFineGrainedAtomicMap(cache2, "testRollbackAndThenCommit", true);
      assertMap(expectedMap, map2);
      assertMap(expectedMap, map1);

      tm(0, "atomic").begin();
      map1.put("k3", "v3");
      map1.put("k4", "v4");
      tm(0, "atomic").commit();

      expectedMap = createMap(new Object[]{"k3", "k4"},
                              new Object[]{"v3", "v4"});
      assertMap(expectedMap, map2);
      assertMap(expectedMap, map1);
   }

   public void testCreateMapInTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      tm(0, "atomic").begin();
      FineGrainedAtomicMap<String, String> map1 = getFineGrainedAtomicMap(cache1, "testCreateMapInTx", true);
      map1.put("k1", "v1");
      tm(0, "atomic").commit();

      Map<Object, Object> expectedMap = createMap(new Object[]{"k1"}, new Object[]{"v1"});
      assertMap(expectedMap, map1);

      final FineGrainedAtomicMap<String, String> map2 = getFineGrainedAtomicMap(cache2, "testCreateMapInTx", true);
      assertMap(expectedMap, map2);
   }

   @SuppressWarnings("UnusedDeclaration")
   public void testNoTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1, "testNoTx", true);
      map.put("existing", "existing");
      map.put("blah", "blah");

      Map<Object, Object> expectedMap = createMap(new Object[]{"existing", "blah"},
                                                  new Object[]{"existing", "blah"});
      assertMap(expectedMap, map);
   }

   @SuppressWarnings("UnusedDeclaration")
   public void testReadUncommittedValues() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");

      tm(cache1).begin();
      map.put("key one", "value one");
      map.put("blah", "blah");

      Map<Object, Object> expectedMap = createMap(new Object[]{"key one", "blah"},
                                                  new Object[]{"value one", "blah"});

      assertMap(expectedMap, map);

      FineGrainedAtomicMap<String, String> sameAsMap = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      assertMap(expectedMap, sameAsMap);
      tm(cache1).commit();

      assertMap(expectedMap, map);
      assertMap(expectedMap, sameAsMap);

      expectedMap = createMap(new Object[]{"blah"},
                              new Object[]{"blah"});
      //now remove one of the elements in a transaction:
      tm(cache1).begin();
      map = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      String removed = map.remove("key one");
      assertEquals("Wrong value removed.", "value one", removed);
      assertNotContainsKey(map, "key one");
      assertNotContainsValue(map, "value one");
      System.out.println(map.values());
      assertMap(expectedMap, map);
      tm(cache1).commit();

      //verify state after commit:
      map = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      removed = map.remove("key one");
      assertNull("Wrong value removed.", removed);
      assertNotContainsKey(map, "key one");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);

      expectedMap = createMap(new Object[]{"key one", "blah"},
                              new Object[]{"value one", "blah"});

      //add the removed element back:
      tm(cache1).begin();
      map = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      map.put("key one", "value one");
      tm(cache1).commit();
      assertMap(expectedMap, map);

      expectedMap = createMap(new Object[]{"key one", "blah"},
                              new Object[]{"value two", "blah"});

      //now test for element replacement:
      tm(cache1).begin();
      map = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      map.put("key one", "value two");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);
      tm(cache1).commit();

      //verify state after commit:
      map = getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      assertNotContainsValue(map, "value one");
      assertMap(expectedMap, map);
   }

   @SuppressWarnings("UnusedDeclaration")
   public void testCommitReadUncommittedValues() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1, "testCommitReadUncommittedValues");
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

      Map<Object, Object> expectedMap = createMap(new Object[]{"key one", "blah", "existing", "hey"},
                                                  new Object[]{"value one", "toronto", "existing", "blah"});
      assertMap(expectedMap, map);

      FineGrainedAtomicMap<String, String> sameAsMap = getFineGrainedAtomicMap(cache1, "testCommitReadUncommittedValues");
      assertMap(expectedMap, sameAsMap);
      tm(cache1).commit();

      assertMap(expectedMap, map);
      assertMap(expectedMap, sameAsMap);
   }

   public void testConcurrentTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final TransactionManager tm1 = tm(cache1);
      final TransactionManager tm2 = tm(cache2);

      final FineGrainedAtomicMap<String, String> map1 = getFineGrainedAtomicMap(cache1, "testConcurrentTx", true);
      Map<Object, Object> expectedMap = createMap(new Object[]{"k1"}, new Object[]{"initial"});

      tm1.begin();
      map1.put("k1", "initial");
      tm1.commit();

      final FineGrainedAtomicMap<String, String> map2 = getFineGrainedAtomicMap(cache2, "testConcurrentTx", false);

      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);

      Thread t1 = fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm1.begin();
               map1.put("k1", "tx1Value");
               tm1.commit();
            } catch (Exception e) {
               log.error(e);
            }
         }
      }, false);

      Thread t2 = fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm2.begin();
               map2.put("k2", "tx2Value");
               tm2.commit();
            } catch (Exception e) {
               log.error(e);
            }
         }
      }, false);

      t2.join();
      t1.join();

      expectedMap = createMap(new Object[]{"k1", "k2"},
                              new Object[]{"tx1Value", "tx2Value"});
      assertMap(expectedMap, map1);
      assertMap(expectedMap, map2);
   }

   public void testReplicationPutCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1, "testReplicationPutCommit");
      Map<Object, Object> expectedMap = createMap(new Object[]{"existing", "blah"},
                                                  new Object[]{"existing", "blah"});

      tm(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      tm(cache1).commit();

      assertMap(expectedMap, map);

      final FineGrainedAtomicMap<Object, Object> other = getFineGrainedAtomicMap(cache2, "testReplicationPutCommit", false);
      assertMap(expectedMap, other);

      //ok, do another tx with delta changes
      tm(cache2).begin();
      other.put("existing", "not existing");
      other.put("not existing", "peace on Earth");
      tm(cache2).commit();

      expectedMap = createMap(new Object[]{"blah", "existing", "not existing"},
                              new Object[]{"blah", "not existing", "peace on Earth"});
      assertMap(expectedMap, map);
      assertMap(expectedMap, other);
   }

   public void testReplicationRemoveCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1, "testReplicationRemoveCommit");

      Map<Object, Object> expectedMap = createMap(new Object[]{"existing", "blah"},
                                                  new Object[]{"existing", "blah"});

      tm(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      tm(cache1).commit();

      assertMap(expectedMap, map);

      final FineGrainedAtomicMap<Object, Object> other = getFineGrainedAtomicMap(cache2, "testReplicationRemoveCommit", false);
      assertMap(expectedMap, other);

      //ok, do another tx with delta changes
      tm(cache2).begin();
      String removed = map.remove("existing");
      assertEquals("Wrong value removed from map '" + map + "'.", "existing", removed);
      tm(cache2).commit();

      expectedMap = createMap(new Object[]{"blah"},
                              new Object[]{"blah"});
      assertMap(expectedMap, map);
      assertMap(expectedMap, other);
   }

   public void testReplicationPutAndClearCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final FineGrainedAtomicMap<String, String> map = getFineGrainedAtomicMap(cache1, "map");
      final FineGrainedAtomicMap<String, String> map2 = getFineGrainedAtomicMap(cache2, "map", false);
      Map<Object, Object> expectedMap = createMap(new Object[]{"existing", "blah"},
                                                  new Object[]{"existing", "blah"});

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

   private void assertSize(Map<?, ?> map, int expectedSize) {
      final int size = map.size();
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

   private void assertMap(Map<?, ?> expected, Map<?, ?> atomicMap) {
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
      assertTrue("Wrong map '" + atomicMap + "'.keySet() content.", atomicMap.keySet().containsAll(expected.keySet()));
      assertTrue("Wrong map '" + atomicMap + "'.values() content.", atomicMap.values().containsAll(expected.values()));
      external:
      for (Entry<?, ?> expectedEntry : expected.entrySet()) {
         for (Entry<?, ?> entry : atomicMap.entrySet()) {
            if (expectedEntry.getKey().equals(entry.getKey()) && expectedEntry.getValue().equals(entry.getValue())) {
               continue external;
            }
         }
         fail("Wrong map '" + atomicMap + "'.entrySet() content. Entry<" + expectedEntry + "> not found!");
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

   private Map<Object, Object> createMap(Object[] keys, Object[] values) {
      assertEquals("Wrong parameters in createMap() method.", keys.length, values.length);
      Map<Object, Object> map = new HashMap<Object, Object>();
      for (int i = 0; i < keys.length; ++i) {
         map.put(keys[i], values[i]);
      }
      return map;
   }
}
