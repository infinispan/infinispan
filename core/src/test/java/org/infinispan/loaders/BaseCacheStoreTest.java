/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders;

import static java.util.Collections.emptySet;
import static org.testng.AssertJUnit.assertEquals;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * This is a base class containing various unit tests for each and every different CacheStore implementations. If you
 * need to add Cache/CacheManager tests that need to be run for each cache store/loader implementation, then use
 * BaseCacheStoreFunctionalTest.
 */
@SuppressWarnings("unchecked")
// this needs to be here for the test to run in an IDE
@Test(groups = "unit", testName = "loaders.BaseCacheStoreTest")
public abstract class BaseCacheStoreTest extends AbstractInfinispanTest {

   protected abstract CacheStore createCacheStore() throws Exception;

   protected CacheStore cs;

   protected TransactionFactory gtf = new TransactionFactory();

   protected BaseCacheStoreTest() {
      gtf.init(false, false, true, false);
   }

   @BeforeMethod
   public void setUp() throws Exception {
      try {
         cs = createCacheStore();
         assert (cs.getCacheStoreConfig()==null || cs.getCacheStoreConfig().isPurgeSynchronously()) : "Cache store tests expect purgeSynchronously to be enabled";
      } catch (Exception e) {
         //in IDEs this won't be printed which makes debugging harder
         e.printStackTrace();
         throw e;
      }
   }

   @AfterMethod
   public void tearDown() throws CacheLoaderException {
      try {
         if (cs != null) {
            cs.clear();
            cs.stop();
         }
      } finally {
         cs = null;
      }
   }

   @AfterMethod(alwaysRun = false)
   public void assertNoLocksHeld(Method m) {
      //doesn't really make sense to add a subclass for this check only
      if (cs instanceof LockSupportCacheStore) {
         int totalLockCount = ((LockSupportCacheStore) cs).getTotalLockCount();
         assert totalLockCount == 0 :
               "Lock count for test method " + m.getName() + " is "
                     + totalLockCount;
      }
   }

   /**
    * @return a mock cache for use with the cache store impls
    */
   protected Cache getCache() {
      return AbstractCacheStoreTest.mockCache("mockCache-" + getClass().getName());
   }

   /**
    * @return a mock marshaller for use with the cache store impls
    */
   protected StreamingMarshaller getMarshaller() {
      return new TestObjectStreamMarshaller(false);
   }

   public void testLoadAndStoreImmortal() throws CacheLoaderException {
      assert !cs.containsKey("k");
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v");
      cs.store(se);

      assert cs.load("k").getValue().equals("v");
      assert cs.load("k").getLifespan() == -1;
      assert cs.load("k").getMaxIdle() == -1;
      assert !cs.load("k").isExpired(System.currentTimeMillis());
      assert cs.containsKey("k");

      boolean removed = cs.remove("k2");
      assert !removed;
   }

   public void testLoadAndStoreWithLifespan() throws Exception {
      assert !cs.containsKey("k");

      long lifespan = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v", lifespan);
      cs.store(se);

      assert cs.containsKey("k");
      InternalCacheEntry ice = cs.load("k");
      assertCorrectExpiry(ice, "v", lifespan, -1, false);
      ice = cs.loadAll().iterator().next();
      assertCorrectExpiry(ice, "v", lifespan, -1, false);

      lifespan = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", lifespan);
      cs.store(se);
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assert !cs.containsKey("k");
      assert cs.loadAll().isEmpty();
   }

   private void assertCorrectExpiry(InternalCacheEntry ice, String value, long lifespan, long maxIdle, boolean expired) {
      assert ice != null : "Cache entry is null";
      assert Util.safeEquals(ice.getValue(), value) : ice.getValue() + " was not " + value;
      assert ice.getLifespan() == lifespan : ice.getLifespan() + " was not " + lifespan;
      assert ice.getMaxIdle() == maxIdle : ice.getMaxIdle() + " was not " + maxIdle;
      if (lifespan > -1) assert ice.getCreated() > -1 : "Created is -1 when maxIdle is set";
      if (maxIdle > -1) assert ice.getLastUsed() > -1 : "LastUsed is -1 when maxIdle is set";
      assert expired == ice.isExpired(System.currentTimeMillis()) : "isExpired() is not " + expired;
   }


   public void testLoadAndStoreWithIdle() throws Exception {
      assert !cs.containsKey("k");

      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v", -1, idle);
      cs.store(se);

      assert cs.containsKey("k");
      InternalCacheEntry ice = cs.load("k");
      assertCorrectExpiry(ice, "v", -1, idle, false);
      ice = cs.loadAll().iterator().next();
      assertCorrectExpiry(ice, "v", -1, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", -1, idle);
      cs.store(se);
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assert !cs.containsKey("k");
      assert cs.loadAll().isEmpty();
   }

   protected void assertEventuallyExpires(String key) throws Exception {
      assert cs.load(key) == null;
   }

   protected void purgeExpired() throws CacheLoaderException {
      cs.purgeExpired();
   }

   public void testLoadAndStoreWithLifespanAndIdle() throws Exception {
      assert !cs.containsKey("k");

      long lifespan = 200000;
      long idle = 120000;
      InternalCacheEntry se = TestInternalCacheEntryFactory.create("k", "v", lifespan, idle);
      cs.store(se);

      assert cs.containsKey("k");
      InternalCacheEntry ice = cs.load("k");
      assertCorrectExpiry(ice, "v", lifespan, idle, false);
      ice = cs.loadAll().iterator().next();
      assertCorrectExpiry(ice, "v", lifespan, idle, false);

      idle = 1;
      se = TestInternalCacheEntryFactory.create("k", "v", lifespan, idle);
      cs.store(se);
      Thread.sleep(100);
      purgeExpired();
      assert se.isExpired(System.currentTimeMillis());
      assertEventuallyExpires("k");
      assert !cs.containsKey("k");
      assert cs.loadAll().isEmpty();
   }

   public void testStopStartDoesNotNukeValues() throws InterruptedException, CacheLoaderException {
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");

      long lifespan = 1;
      long idle = 1;
      InternalCacheEntry se1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry se2 = TestInternalCacheEntryFactory.create("k2", "v2");
      InternalCacheEntry se3 = TestInternalCacheEntryFactory.create("k3", "v3", -1, idle);
      InternalCacheEntry se4 = TestInternalCacheEntryFactory.create("k4", "v4", lifespan, idle);

      cs.store(se1);
      cs.store(se2);
      cs.store(se3);
      cs.store(se4);

      sleepForStopStartTest();

      cs.stop();
      cs.start();
      assert se1.isExpired(System.currentTimeMillis());
      assert cs.load("k1") == null;
      assert !cs.containsKey("k1");
      assert cs.load("k2") != null;
      assert cs.containsKey("k2");
      assert cs.load("k2").getValue().equals("v2");
      assert se3.isExpired(System.currentTimeMillis());
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
      assert se3.isExpired(System.currentTimeMillis());
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
   }

   protected void sleepForStopStartTest() throws InterruptedException {
      Thread.sleep(100);
   }


   public void testOnePhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      GlobalTransaction tx = gtf.newGlobalTransaction(null, true);
      cs.prepare(mods, tx, true);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));
      cs.prepare(mods, tx, true);
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
   }

   public void testTwoPhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");

      cs.commit(tx);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");

      cs.commit(tx);

      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
   }


   public void testRollback() throws CacheLoaderException {

      cs.store(TestInternalCacheEntryFactory.create("old", "old"));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");

      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

   public void testRollbackFromADifferentThreadReusingTransactionKey() throws CacheLoaderException, InterruptedException {

      cs.store(TestInternalCacheEntryFactory.create("old", "old"));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      final GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      cs.prepare(mods, tx, false);

      Thread t = new Thread(new Runnable() {
         @Override
         public void run() {
            cs.rollback(tx);
         }
      });

      t.start();
      t.join();

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("old");

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);

      Thread t2 = new Thread(new Runnable() {
         @Override
         public void run() {
            cs.rollback(tx);
         }
      });

      t2.start();
      t2.join();
      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

   public void testCommitAndRollbackWithoutPrepare() throws CacheLoaderException {
      cs.store(TestInternalCacheEntryFactory.create("old", "old"));
      GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      cs.commit(tx);
      cs.store(TestInternalCacheEntryFactory.create("old", "old"));
      cs.rollback(tx);

      assert cs.containsKey("old");
   }

   public void testPreload() throws Exception {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2"));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3"));

      Set<InternalCacheEntry> set = cs.loadAll();

      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testPreloadWithMaxSize() throws CacheLoaderException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2"));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3"));

      Set<InternalCacheEntry> set = cs.load(2);

      assertEquals(2, set.size());
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.size() == 1;
   }

   public void testStoreAndRemoveAll() throws CacheLoaderException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2"));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3"));
      cs.store(TestInternalCacheEntryFactory.create("k4", "v4"));


      Set<InternalCacheEntry> set = cs.loadAll();

      assert set.size() == 4;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      expected.add("k4");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();

      Set toRemove = new HashSet();
      toRemove.add("k1");
      toRemove.add("k2");
      toRemove.add("k3");
      cs.removeAll(toRemove);

      set = cs.loadAll();
      assert set.size() == 1;
      set.remove("k4");
      assert expected.isEmpty();
   }

   public void testPurgeExpired() throws Exception {
      // Increased lifespan and idle timeouts to accommodate slower cache stores
      long lifespan = 6000;
      long idle = 4000;
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", lifespan));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", -1, idle));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3", lifespan, idle));
      cs.store(TestInternalCacheEntryFactory.create("k4", "v4", -1, -1)); // immortal entry
      cs.store(TestInternalCacheEntryFactory.create("k5", "v5", lifespan * 1000, idle * 1000)); // long life mortal entry
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
      assert cs.containsKey("k4");
      assert cs.containsKey("k5");

      Thread.sleep(lifespan + 10);
      purgeExpired();

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("k4");
      assert cs.containsKey("k5");
   }

   public void testLoadKeys() throws CacheLoaderException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v"));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v"));
      cs.store(TestInternalCacheEntryFactory.create("k4", "v"));
      cs.store(TestInternalCacheEntryFactory.create("k5", "v"));

      Set<Object> s = cs.loadAllKeys(null);
      assert s.size() == 5 : "Expected 5 keys, was " + s;

      s = cs.loadAllKeys(emptySet());
      assert s.size() == 5 : "Expected 5 keys, was " + s;

      Set<Object> excl = Collections.<Object>singleton("k3");
      s = cs.loadAllKeys(excl);
      assert s.size() == 4 : "Expected 4 keys but was " + s;

      assert !s.contains("k3");
   }

   public void testStreamingAPI() throws IOException, CacheLoaderException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2"));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3"));

      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutput oo = marshaller.startObjectOutput(out, false, 12);
      try {
         cs.toStream(new UnclosableObjectOutputStream(oo));
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
         cs.clear();
      }

      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         cs.fromStream(new UnclosableObjectInputStream(oi));
      } finally {
         marshaller.finishObjectInput(oi);
         in.close();
      }

      Set<InternalCacheEntry> set = cs.loadAll();
      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testStreamingAPIReusingStreams() throws IOException, CacheLoaderException {
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2"));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3"));

      StreamingMarshaller marshaller = getMarshaller();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] dummyStartBytes = {1, 2, 3, 4, 5, 6, 7, 8};
      byte[] dummyEndBytes = {8, 7, 6, 5, 4, 3, 2, 1};
      ObjectOutput oo = marshaller.startObjectOutput(out, false ,12);
      try {
         oo.write(dummyStartBytes);
         cs.toStream(new UnclosableObjectOutputStream(oo));
         oo.flush();
         oo.write(dummyEndBytes);
      } finally {
         marshaller.finishObjectOutput(oo);
         out.close();
         cs.clear();
      }

      // first pop the start bytes
      byte[] dummy = new byte[8];
      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      ObjectInput oi = marshaller.startObjectInput(in, false);
      try {
         int bytesRead = oi.read(dummy, 0, 8);
         assert bytesRead == 8;
         for (int i = 1; i < 9; i++) assert dummy[i - 1] == i : "Start byte stream corrupted!";
         cs.fromStream(new UnclosableObjectInputStream(oi));
         bytesRead = oi.read(dummy, 0, 8);
         assert bytesRead == 8;
         for (int i = 8; i > 0; i--) assert dummy[8 - i] == i : "Start byte stream corrupted!";
      } finally {
         marshaller.finishObjectInput(oi);
         in.close();
      }

      Set<InternalCacheEntry> set = cs.loadAll();
      assert set.size() == 3;
      Set expected = new HashSet();
      expected.add("k1");
      expected.add("k2");
      expected.add("k3");
      for (InternalCacheEntry se : set) assert expected.remove(se.getKey());
      assert expected.isEmpty();
   }

   public void testConfigFile() throws Exception {
      Class<? extends CacheLoaderConfig> cfgClass = cs.getConfigurationClass();
      CacheLoaderConfig clc = Util.getInstance(cfgClass);
      assert clc.getCacheLoaderClassName().equals(cs.getClass().getName()) : "Cache loaders doesn't provide a proper configuration type that is capable of creating the loaders!";
   }

   public void testConcurrency() throws Exception {
      int numThreads = 3;
      final int loops = 500;
      final String[] keys = new String[10];
      final String[] values = new String[10];
      for (int i = 0; i < 10; i++) keys[i] = "k" + i;
      for (int i = 0; i < 10; i++) values[i] = "v" + i;


      final Random r = new Random();
      final List<Exception> exceptions = new LinkedList<Exception>();

      final Runnable store = new Runnable() {
         @Override
         public void run() {
            try {
               int randomInt = r.nextInt(10);
               cs.store(TestInternalCacheEntryFactory.create(keys[randomInt], values[randomInt]));
            } catch (Exception e) {
               exceptions.add(e);
            }
         }
      };

      final Runnable remove = new Runnable() {
         @Override
         public void run() {
            try {
               cs.remove(keys[r.nextInt(10)]);
            } catch (Exception e) {
               exceptions.add(e);
            }
         }
      };

      final Runnable get = new Runnable() {
         @Override
         public void run() {
            try {
               int randomInt = r.nextInt(10);
               InternalCacheEntry se = cs.load(keys[randomInt]);
               assert se == null || se.getValue().equals(values[randomInt]);
               cs.loadAll();
            } catch (Exception e) {
               exceptions.add(e);
            }
         }
      };

      Thread[] threads = new Thread[numThreads];

      for (int i = 0; i < numThreads; i++) {
         threads[i] = new Thread(getClass().getSimpleName() + "-" + i) {
            @Override
            public void run() {
               for (int i = 0; i < loops; i++) {
                  store.run();
                  remove.run();
                  get.run();
               }
            }
         };
      }

      for (Thread t : threads) t.start();
      for (Thread t : threads) t.join();

      if (!exceptions.isEmpty()) throw exceptions.get(0);
   }

   public void testReplaceExpiredEntry() throws Exception {
      final long startTime = System.currentTimeMillis();
      final long lifespan = 3000;
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1", lifespan));
      while (true) {
         InternalCacheEntry entry = cs.load("k1");
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assert entry.getValue().equals("v1");
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (cs.load("k1") == null) break;
      }

      assert null == cs.load("k1");

      cs.store(TestInternalCacheEntryFactory.create("k1", "v2", lifespan));
      while (true) {
         InternalCacheEntry entry = cs.load("k1");
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assert entry.getValue().equals("v2");
         Thread.sleep(100);
      }

      // Make sure that in the next 20 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 20000) {
         if (cs.load("k1") == null) break;
      }

      assert null == cs.load("k1");
   }

   public void testLoadAndStoreMarshalledValues() throws CacheLoaderException {
      MarshalledValue key = new MarshalledValue(new Pojo().role("key"), true, getMarshaller());
      MarshalledValue key2 = new MarshalledValue(new Pojo().role("key2"), true, getMarshaller());
      MarshalledValue value = new MarshalledValue(new Pojo().role("value"), true, getMarshaller());

      assert !cs.containsKey(key);
      InternalCacheEntry se = TestInternalCacheEntryFactory.create(key, value);
      cs.store(se);

      assert cs.load(key).getValue().equals(value);
      assert cs.load(key).getLifespan() == -1;
      assert cs.load(key).getMaxIdle() == -1;
      assert !cs.load(key).isExpired(System.currentTimeMillis());
      assert cs.containsKey(key);

      boolean removed = cs.remove(key2);
      assert !removed;

      assert cs.remove(key);
   }

   public static class Pojo implements Serializable {

      private String role;

      public Pojo role(String role) {
         this.role = role;
         return this;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Pojo pojo = (Pojo) o;

         if (role != null ? !role.equals(pojo.role) : pojo.role != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return role != null ? role.hashCode() : 0;
      }
   }

}
