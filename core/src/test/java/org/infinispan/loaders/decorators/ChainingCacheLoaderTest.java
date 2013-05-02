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
package org.infinispan.loaders.decorators;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.io.UnclosableObjectInputStream;
import org.infinispan.io.UnclosableObjectOutputStream;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "loaders.decorators.ChainingCacheLoaderTest")
public class ChainingCacheLoaderTest extends BaseCacheStoreTest {

   DummyInMemoryCacheStore store1, store2;
   DummyInMemoryCacheStore[] stores;  // for convenient iteration
   private static final long lifespan = 6000000;

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      ChainingCacheStore store = new ChainingCacheStore();
      CacheStoreConfig cfg = new DummyInMemoryCacheStore.Cfg()
         .storeName("ChainingCacheLoaderTest_instance1")
         .purgeOnStartup(false)
         .fetchPersistentState(false);
      store1 = new DummyInMemoryCacheStore();
      store1.init(cfg, getCache(), new TestObjectStreamMarshaller());

      ConfigurationBuilder builder = new ConfigurationBuilder();
      LegacyConfigurationAdaptor.adapt(Thread.currentThread().getContextClassLoader(), builder, cfg);
      store.addCacheLoader(store1, builder.build().loaders().cacheLoaders().get(0));

      store2 = new DummyInMemoryCacheStore();
      // set store2 up for streaming
      cfg = new DummyInMemoryCacheStore.Cfg()
         .storeName("ChainingCacheLoaderTest_instance2")
         .purgeOnStartup(false)
         .fetchPersistentState(true);
      store2.init(cfg, getCache(), new TestObjectStreamMarshaller());

      builder = new ConfigurationBuilder();
      LegacyConfigurationAdaptor.adapt(Thread.currentThread().getContextClassLoader(), builder, cfg);
      store.addCacheLoader(store2, builder.build().loaders().cacheLoaders().get(0));

      stores = new DummyInMemoryCacheStore[]{store1, store2};

      store.start();

      return store;
   }

   @AfterMethod
   public void afterMethod() {
      if (store1 != null)
         store1.clear();
      if (store2 != null)
         store2.clear();
   }

   public void testPropagatingWrites() throws Exception {
      // put something in the store
      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan));

      int i = 1;
      for (CacheStore s : stores) {
         assert s.containsKey("k1") : "Key k1 missing on store " + i;
         assert s.containsKey("k2") : "Key k2 missing on store " + i;
         assert s.load("k1").getValue().equals("v1");
         assert s.load("k2").getValue().equals("v2");
         assert s.load("k1").getLifespan() == -1;
         assert s.load("k2").getLifespan() == lifespan;
         i++;
      }

      cs.remove("k1");

      for (CacheStore s : stores) {
         assert !s.containsKey("k1");
         assert s.containsKey("k2");
         assert s.load("k1") == null;
         assert s.load("k2").getValue().equals("v2");
         assert s.load("k2").getLifespan() == lifespan;
      }

      cs.clear();

      for (CacheStore s : stores) {
         assert !s.containsKey("k1");
         assert !s.containsKey("k2");
         assert s.load("k1") == null;
         assert s.load("k2") == null;
      }

      cs.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      cs.store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan));
      cs.store(TestInternalCacheEntryFactory.create("k3", "v3", 1000)); // short lifespan!

      for (CacheStore s : stores) {
         assert s.containsKey("k1");
         assert s.containsKey("k2");
         assert s.containsKey("k3");
      }

      Thread.sleep(1100);

      cs.purgeExpired();

      for (CacheStore s : stores) {
         assert s.containsKey("k1");
         assert s.containsKey("k2");
         assert !s.containsKey("k3");
      }
   }

   public void testGetsFromMultipleSrcs() throws Exception {

      assert cs.load("k1") == null;
      assert cs.load("k2") == null;
      assert cs.load("k3") == null;
      assert cs.load("k4") == null;

      // k1 is on store1
      store1.store(TestInternalCacheEntryFactory.create("k1", "v1"));

      assertEquals(cs.loadAll().size(), 1);
      // k2 is on store2
      store2.store(TestInternalCacheEntryFactory.create("k2", "v2"));
      assertEquals(cs.loadAll().size(), 2);

      // k3 is on both
      store1.store(TestInternalCacheEntryFactory.create("k3", "v3"));
      assertEquals(cs.loadAll().size(), 3);
      store2.store(TestInternalCacheEntryFactory.create("k3", "v3"));
      assertEquals(cs.loadAll().size(), 3);

      // k4 is on neither

      assert cs.load("k1").getValue().equals("v1");
      assert cs.load("k2").getValue().equals("v2");
      assert cs.load("k3").getValue().equals("v3");
      assert cs.load("k4") == null;

      Set<InternalCacheEntry> all = cs.loadAll();

      assertEquals(all.size(),3);
      Set<Object> expectedKeys = new HashSet<Object>();
      expectedKeys.add("k1");
      expectedKeys.add("k2");
      expectedKeys.add("k3");
      for (InternalCacheEntry a : all) assert expectedKeys.remove(a.getKey());

      assert expectedKeys.isEmpty();

      cs.remove("k3");

      assert !store1.containsKey("k3");
      assert !store2.containsKey("k3");
   }

   public void testPropagatingOnePhaseCommit() throws Exception {
      List<Modification> list = new LinkedList<Modification>();
      list.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      list.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan)));
      list.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));
      list.add(new Remove("k3"));
      list.add(new Clear());
      list.add(new Store(TestInternalCacheEntryFactory.create("k4", "v4")));
      list.add(new Store(TestInternalCacheEntryFactory.create("k5", "v5", lifespan)));
      list.add(new Store(TestInternalCacheEntryFactory.create("k6", "v6")));
      list.add(new Remove("k6"));
      GlobalTransaction t = gtf.newGlobalTransaction(null, false);
      cs.prepare(list, t, true);

      CacheStore[] allStores = new CacheStore[]{cs, store1, store2}; // for iteration
      for (int i = 1; i < 7; i++) {
         if (i < 4 || i == 6) {
            // these have been deleted
            for (CacheStore s : allStores) assert !s.containsKey("k" + i) : "Failed on k" + i;
         } else {
            for (CacheStore s : allStores) {
               assert s.containsKey("k" + i);
               assert s.load("k" + i).getValue().equals("v" + i);
               assert s.load("k" + i).getLifespan() == (i == 5 ? lifespan : -1);
            }
         }
      }

      cs.clear();

      for (int i = 1; i < 7; i++) {
         for (CacheStore s : allStores) assert !s.containsKey("k" + i);
      }
   }

   public void testPropagatingTwoPhaseCommit() throws Exception {
      List<Modification> list = new LinkedList<Modification>();
      list.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      list.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan)));
      list.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));
      list.add(new Remove("k3"));
      list.add(new Clear());
      list.add(new Store(TestInternalCacheEntryFactory.create("k4", "v4")));
      list.add(new Store(TestInternalCacheEntryFactory.create("k5", "v5", lifespan)));
      list.add(new Store(TestInternalCacheEntryFactory.create("k6", "v6")));
      list.add(new Remove("k6"));
      GlobalTransaction tx = gtf.newGlobalTransaction(null, false);
      cs.prepare(list, tx, false);

      CacheStore[] allStores = new CacheStore[]{cs, store1, store2}; // for iteration

      for (int i = 1; i < 7; i++) {
         for (CacheStore s : allStores) assert !s.containsKey("k" + i);
      }

      cs.commit(tx);

      for (int i = 1; i < 7; i++) {
         if (i < 4 || i == 6) {
            // these have been deleted
            for (CacheStore s : allStores) assert !s.containsKey("k" + i);
         } else {
            for (CacheStore s : allStores) {
               assert s.containsKey("k" + i);
               assert s.load("k" + i).getValue().equals("v" + i);
               assert s.load("k" + i).getLifespan() == (i == 5 ? lifespan : -1);
            }
         }
      }
   }


   public void testPropagatingStreams() throws IOException, CacheLoaderException {
      store2.store(TestInternalCacheEntryFactory.create("k1", "v1"));
      store2.store(TestInternalCacheEntryFactory.create("k2", "v2", lifespan));

      assert cs.containsKey("k1");
      assert cs.containsKey("k2");

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(byteStream);
      cs.toStream(new UnclosableObjectOutputStream(oos));
      oos.close();
      byteStream.close();
      cs.clear();

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      for (CacheStore s : stores) {
         assert !s.containsKey("k1");
         assert !s.containsKey("k2");
      }

      cs.fromStream(new UnclosableObjectInputStream(new ObjectInputStream(new ByteArrayInputStream(byteStream.toByteArray()))));

      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.load("k1").getValue().equals("v1");
      assert cs.load("k2").getValue().equals("v2");
      assert cs.load("k1").getLifespan() == -1;
      assert cs.load("k2").getLifespan() == lifespan;

      assert store2.containsKey("k1");
      assert store2.containsKey("k2");
      assert store2.load("k1").getValue().equals("v1");
      assert store2.load("k2").getValue().equals("v2");
      assert store2.load("k1").getLifespan() == -1;
      assert store2.load("k2").getLifespan() == lifespan;

      // store 1 has not been set up to support fetching state
      assert !store1.containsKey("k1");
      assert !store1.containsKey("k2");
   }

   @Override
   public void testConfigFile() {
      // no op
   }
}

