/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.atomic;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * @author Vladimir Blagojevic (C) 2011 Red Hat Inc.
 * @author Sanne Grinovero (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "atomic.FineGrainedAtomicMapAPITest")
public class FineGrainedAtomicMapAPITest extends MultipleCacheManagersTest {

   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.transaction()
                  .transactionMode(TransactionMode.TRANSACTIONAL)
                  .lockingMode(LockingMode.PESSIMISTIC)
                  .locking().lockAcquisitionTimeout(100l);
      createCluster( configurationBuilder, 2 );
   }

   @Test(enabled=true)
   public void testMultipleTx() throws Exception{
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      final TransactionManager tm2 = TestingUtil.getTransactionManager(cache2);

      final FineGrainedAtomicMap<String, String> map1 = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testMultipleTx", true);
      final FineGrainedAtomicMap<String, String> map2 = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testMultipleTx", false);
      assert map2.size() == 0 && map1.size() == 0;

      tm1.begin();
      map1.put("k1", "initial");
      tm1.commit();

      assert map2.size() == 1 && map1.size() == 1;

      tm1.begin();
      map1.put("k1", "v1");
      map1.put("k2", "v2");
      map1.put("k3", "v3");
      tm1.commit();  

      assert map1.size() == 3;
      assert map2.size() == 3;

      tm1.begin();
      map1.put("k4", "v4");
      map1.put("k5", "v5");
      map1.put("k6", "v6");
      tm1.commit();

      assert map2.get("k1").equals("v1");
      assert map2.get("k2").equals("v2");
      assert map2.get("k3").equals("v3");
      assert map2.get("k4").equals("v4");
      assert map2.get("k5").equals("v5");
      assert map2.get("k6").equals("v6");
      
      assert map1.get("k1").equals("v1");
      assert map1.get("k2").equals("v2");
      assert map1.get("k3").equals("v3");
      assert map1.get("k4").equals("v4");
      assert map1.get("k5").equals("v5");
      assert map1.get("k6").equals("v6");
   }

   @Test(enabled=true)
   public void testSizeOnCache() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assert cache1.size() == 0;
      cache1.put("Hi", "Someone");
      assert cache1.size() == 1;

      tm(0, "atomic").begin();
      assert cache1.size() == 1;
      cache1.put("Need", "Read Consistency");
      assert cache1.size() == 2;
      tm(0, "atomic").commit();
      assert cache1.size() == 2;

      tm(0, "atomic").begin();
      assert cache1.size() == 2;
      cache1.put("Need Also", "Speed");
      assert cache1.size() == 3;
      tm(0, "atomic").rollback();
      assert cache1.size() == 2;

      FineGrainedAtomicMap<Object,Object> atomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testSizeOnCache", true);
      assert cache1.size() == 3;
      atomicMap.put("mm", "nn");
      assert cache1.size() == 3;

      tm(0, "atomic").begin();
      assert cache1.size() == 3;
      atomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testSizeOnCache-second", true);
      assert cache1.size() == 4;
      atomicMap.put("mm", "nn");
      assert cache1.size() == 4 : "Cache size is actually " + cache1.size();
      tm(0, "atomic").commit();
      assert cache1.size() == 4;

      tm(0, "atomic").begin();
      assert cache1.size() == 4;
      atomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testSizeOnCache-third", true);
      assert cache1.size() == 5;
      atomicMap.put("mm", "nn");
      assert cache1.size() == 5 : "Cache size is actually " + cache1.size();
      atomicMap.put("ooo", "weird!");
      assert cache1.size() == 5 : "Cache size is actually " + cache1.size();
      atomicMap = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testSizeOnCache-onemore", true);
      assert cache1.size() == 6 : "Cache size is actually " + cache1.size();
      atomicMap.put("even less?", "weird!");
      assert cache1.size() == 6 : "Cache size is actually " + cache1.size();
      tm(0, "atomic").rollback();
      assert cache1.size() == 4;
   }

   @Test(enabled=true)
   public void testConcurrentReadsOnExistingMap() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assert cache1.size() == 0;
      final FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
      map.put("the-1", "my preciousss");
      tm(0, "atomic").begin();
      assert "my preciousss".equals(map.get("the-1"));
      final AtomicBoolean allok = new AtomicBoolean(false);
      map.put("the-2", "a minor");

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(0, "atomic").begin();
               FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
               assert "my preciousss".equals(map.get("the-1"));
               assert ! map.containsKey("the-2");
               tm(0, "atomic").commit();
               allok.set(true);
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, true);

      tm(0, "atomic").commit();
      assert allok.get();
   }

   @Test(enabled=true)
   public void testConcurrentWritesOnExistingMap() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assert cache1.size() == 0;
      final FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
      map.put("the-1", "my preciousss");
      tm(0, "atomic").begin();
      assert "my preciousss".equals(map.get("the-1"));
      final AtomicBoolean allok = new AtomicBoolean(false);
      map.put("the-2", "a minor");

      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm(0, "atomic").begin();
               FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentReadsOnExistingMap", true);
               assert "my preciousss".equals(map.get("the-1"));
               assert ! map.containsKey("the-2");
               map.put("the-2", "a minor-different"); // We're in pessimistic locking, so this put is going to block
               tm(0, "atomic").commit();
            } catch (org.infinispan.util.concurrent.TimeoutException e) {
               allok.set(true);
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, true);

      tm(0, "atomic").commit();
      assert allok.get();
   }
   
   @Test(enabled=true)
   public void testConcurrentWritesAndIteration() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      assert cache1.size() == 0;
      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentWritesAndIteration", true);
      assert map.size() == 0;
      final AtomicBoolean allOk = new AtomicBoolean(true);
      final CountDownLatch latch = new CountDownLatch(1);
      Thread t1 = fork(new Runnable() {
         @Override
         public void run() {
            try {
               FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentWritesAndIteration", true);
               latch.await();
               for(int i = 0; i< 500; i++){
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
            FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentWritesAndIteration", true);
            try {               
               latch.await();               
               for(int i = 0; i< 500; i++){                  
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
      assert allOk.get() : "iteration raised an exception";
   }


   @Test(enabled=true)
   public void testRollback() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      final FineGrainedAtomicMap<String, String> map1 = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testRollback", true);

      tm(0, "atomic").begin();
      map1.put("k1", "v");
      map1.put("k2", "v2");
      tm(0, "atomic").rollback();
      FineGrainedAtomicMap<Object, Object> instance = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testRollback", true);
      assert !instance.containsKey("k1");

      assert !map1.containsKey("k1");
   }

   @Test(enabled=true,expectedExceptions={IllegalArgumentException.class})
   public void testFineGrainedMapAfterSimpleMap() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");

      AtomicMap<String, String> map = AtomicMapLookup.getAtomicMap(cache1, "testReplicationRemoveCommit");
      FineGrainedAtomicMap<String, String> map2 = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReplicationRemoveCommit");
   }

   @Test(enabled=true)
   public void testRollbackAndThenCommit() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      final FineGrainedAtomicMap<String, String> map1 = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testRollbackAndThenCommit", true);

      tm(0, "atomic").begin();
      map1.put("k1", "v");
      map1.put("k2", "v2");
      tm(0, "atomic").rollback();
      FineGrainedAtomicMap<Object, Object> map2 = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testRollbackAndThenCommit", true);
      assert !map2.containsKey("k1");
      assert !map1.containsKey("k1");

      tm(0, "atomic").begin();
      map1.put("k3", "v3");
      map1.put("k4", "v4");
      tm(0, "atomic").commit();

      assert map1.size() == 2 && map2.size() == 2;
   }

   @Test(enabled=true)
   public void testCreateMapInTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");
      FineGrainedAtomicMap<String, String> map1;
      tm(0, "atomic").begin();
      map1 = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testCreateMapInTx", true);
      map1.put("k1", "v1");
      tm(0, "atomic").commit();

      assert map1.size() == 1;
      assert map1.get("k1").equals("v1");

      final FineGrainedAtomicMap<String, String> map2 = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testCreateMapInTx", true);

      assert map2.size() == 1;
      assert map2.get("k1").equals("v1");
   }

   @Test(enabled=true)
   public void testNoTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testNoTx", true);
      map.put("existing", "existing");
      map.put("blah", "blah");

      assert map.size() == 2;
      assert map.get("blah").equals("blah");
      assert map.containsKey("existing");
   }
   
   @Test(enabled=true)
   public void testReadUncommittedValues() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("key one", "value one");
      map.put("blah", "blah");

      assert "value one".equals(map.get("key one"));
      assert map.size() == 2;
      assert map.keySet().size() == 2;
      Set<String> keySet = map.keySet();
      for (String k : keySet) {
         assert k.equals("key one") || k.equals("blah");
      }
      Collection<String> values = map.values();
      for (String v : values) {
         assert v.equals("value one") || v.equals("blah");
      }
      assert map.containsKey("key one");
      assert map.values().size() == 2;
      assert !map.isEmpty();
      Set<Entry<String,String>> entrySet = map.entrySet();
      for (Entry<String, String> entry : entrySet) {
         if(entry.getKey().equals("key one")) assert entry.getValue().equals("value one");
         if(entry.getKey().equals("blah")) assert entry.getValue().equals("blah");
      }

      FineGrainedAtomicMap<String, String> sameAsMap = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      assert "value one".equals(sameAsMap.get("key one"));
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 2;
      assert map.get("blah").equals("blah");

      //now remove one of the elements in a transaction:
      TestingUtil.getTransactionManager(cache1).begin();
      map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      String removed = map.remove("key one");
      assert "value one".equals(removed);
      assert ! map.containsKey("key one");
      assert ! map.containsValue("value one");
      assert map.size() == 1;
      assert ! map.isEmpty();
      entrySet = map.entrySet();
      assert entrySet.size() == 1;
      for (Entry<String, String> entry : entrySet) {
         assert "blah".equals(entry.getKey());
         assert "blah".equals(entry.getValue());
      }
      TestingUtil.getTransactionManager(cache1).commit();

      //verify state after commit:
      map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      removed = map.remove("key one");
      assert removed == null;
      assert ! map.containsKey("key one");
      assert ! map.containsValue("value one");
      assert map.size() == 1;
      assert ! map.isEmpty();
      entrySet = map.entrySet();
      assert entrySet.size() == 1;
      for (Entry<String, String> entry : entrySet) {
         assert "blah".equals(entry.getKey());
         assert "blah".equals(entry.getValue());
      }

      //add the removed element back:
      TestingUtil.getTransactionManager(cache1).begin();
      map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      map.put("key one", "value one");
      TestingUtil.getTransactionManager(cache1).commit();
      assert map.size() == 2;

      //now test for element replacement:
      TestingUtil.getTransactionManager(cache1).begin();
      map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      map.put("key one", "value two");
      assert map.containsKey("key one");
      assert ! map.containsValue("value one");
      assert map.containsValue("value two");
      assert map.size() == 2;
      assert ! map.isEmpty();
      entrySet = map.entrySet();
      assert entrySet.size() == 2;
      for (Entry<String, String> entry : entrySet) {
         if(entry.getKey().equals("key one")) assert entry.getValue().equals("value two");
         if(entry.getKey().equals("blah")) assert entry.getValue().equals("blah");
      }
      TestingUtil.getTransactionManager(cache1).commit();

      //verify state after commit:
      map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReadUncommittedValues");
      assert map.containsKey("key one");
      assert ! map.containsValue("value one");
      assert map.containsValue("value two");
      assert map.size() == 2;
      assert ! map.isEmpty();
      entrySet = map.entrySet();
      assert entrySet.size() == 2;
      for (Entry<String, String> entry : entrySet) {
         if(entry.getKey().equals("key one")) assert entry.getValue().equals("value two");
         if(entry.getKey().equals("blah")) assert entry.getValue().equals("blah");
      }

   }

   @Test(enabled=true)
   public void testCommitReadUncommittedValues() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testCommitReadUncommittedValues");
      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("hey", "blah");
      TestingUtil.getTransactionManager(cache1).commit();

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("key one", "fake one");
      map.put("key one", "value one");
      map.put("blah", "montevideo");
      map.put("blah", "buenos aires");
      map.remove("blah");
      map.put("blah", "toronto");

      assert "value one".equals(map.get("key one"));
      assert map.size() == 4;
      assert map.keySet().size() == 4;
      Set<String> keySet = map.keySet();
      for (String k : keySet) {
         assert k.equals("key one") || k.equals("blah") || k.equals("existing") || k.equals("hey");
      }      
      Collection<String> values = map.values();
      for (String v : values) {
         assert v.equals("value one") || v.equals("blah") || v.equals("existing") || v.equals("toronto");
      }
      assert map.containsKey("key one");
      assert map.values().size() == 4; 
      assert !map.isEmpty();
      Set<Entry<String,String>> entrySet = map.entrySet();
      for (Entry<String, String> entry : entrySet) {
         if(entry.getKey().equals("key one")) assert entry.getValue().equals("value one");
         if(entry.getKey().equals("blah")) assert entry.getValue().equals("toronto");
         if(entry.getKey().equals("existing")) assert entry.getValue().equals("existing");
         if(entry.getKey().equals("hey")) assert entry.getValue().equals("blah");
      }
      
      FineGrainedAtomicMap<String, String> sameAsMap = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testCommitReadUncommittedValues");
      assert "value one".equals(sameAsMap.get("key one"));
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 4;
      assert map.get("blah").equals("toronto");
   }

   @Test(enabled=true)
   public void testConcurrentTx() throws Exception {
      final Cache<String, Object> cache1 = cache(0, "atomic");
      final Cache<String, Object> cache2 = cache(1, "atomic");

      final TransactionManager tm1 = TestingUtil.getTransactionManager(cache1);
      final TransactionManager tm2 = TestingUtil.getTransactionManager(cache2);

      final FineGrainedAtomicMap<String, String> map1 = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testConcurrentTx",true);
      tm1.begin();
      map1.put("k1", "initial");
      tm1.commit();

      final FineGrainedAtomicMap<String, String> map2 = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testConcurrentTx", false);
      assert map2.size() == 1 && map1.size() == 1;
      Thread t1 = new Thread( new Runnable() {

         @Override
         public void run() {
            try {
               tm1.begin();
               map1.put("k1", "tx1Value");
               tm1.commit();
            } 
            catch (Exception e) {
               log.error(e);
            } 
         }});
      t1.start();

      Thread t2 = new Thread(new Runnable(){

         @Override
         public void run() {
            try {
               tm2.begin();
               map2.put("k2", "tx2Value");  
               tm2.commit();
            } 
            catch (Exception e) {
               log.error(e);
            }
         }});
      t2.start();

      t2.join();
      t1.join();
      assert map2.get("k2").equals("tx2Value");
      assert map2.get("k1").equals("tx1Value");

      assert map1.get("k2").equals("tx2Value");
      assert map1.get("k1").equals("tx1Value");
   }

   
   @Test(enabled=true)
   public void testReplicationPutCommit() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReplicationPutCommit");

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 2;
      assert map.get("blah").equals("blah");
      assert map.containsKey("existing");

      FineGrainedAtomicMap<Object, Object> other = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testReplicationPutCommit", false);
      assert other.size() == 2:" no, other size is " + other.size(); 
      assert other.get("blah").equals("blah");
      assert other.containsKey("blah");

      //ok, do another tx with delta changes
      TestingUtil.getTransactionManager(cache2).begin();
      other.put("existing", "not existing"); 
      other.put("not existing", "peace on Earth"); 
      TestingUtil.getTransactionManager(cache2).commit();

      assert map.size() == 3;
      assert map.get("blah").equals("blah");
      assert map.get("existing").equals("not existing");
      assert map.get("not existing").equals("peace on Earth");

      assert other.size() == 3;
      assert other.get("blah").equals("blah");
      assert other.get("existing").equals("not existing");
      assert other.get("not existing").equals("peace on Earth");
   }

   @Test(enabled=true)
   public void testReplicationRemoveCommit() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "testReplicationRemoveCommit");

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 2;
      assert map.get("blah").equals("blah");
      assert map.containsKey("existing");

      FineGrainedAtomicMap<Object, Object> other = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "testReplicationRemoveCommit", false);
      assert other.size() == 2:" no, other size is " + other.size(); 
      assert other.get("blah").equals("blah");
      assert other.containsKey("blah");

      //ok, do another tx with delta changes
      TestingUtil.getTransactionManager(cache2).begin();
      String removed = map.remove("existing");
      assert removed.equals("existing");
      TestingUtil.getTransactionManager(cache2).commit();

      assert map.size() == 1;
      assert map.get("blah").equals("blah");

      assert other.size() == 1;
      assert other.get("blah").equals("blah");
   }

   @Test(enabled=true)
   public void testReplicationPutAndClearCommit() throws Exception {
      Cache<String, Object> cache1 = cache(0, "atomic");
      Cache<String, Object> cache2 = cache(1, "atomic");

      FineGrainedAtomicMap<String, String> map = AtomicMapLookup.getFineGrainedAtomicMap(cache1, "map");
      FineGrainedAtomicMap<String, String> map2 = AtomicMapLookup.getFineGrainedAtomicMap(cache2, "map", false);

      TestingUtil.getTransactionManager(cache1).begin();
      map.put("existing", "existing");
      map.put("blah", "blah");
      map.size();
      TestingUtil.getTransactionManager(cache1).commit();

      assert map.size() == 2;
      assert map2.size() == 2;

      //ok, do another tx with clear delta changes
      TestingUtil.getTransactionManager(cache2).begin();
      map2.clear();
      TestingUtil.getTransactionManager(cache2).commit();

      assert map.size() == 0;
      assert map2.size() == 0;
   }

}
