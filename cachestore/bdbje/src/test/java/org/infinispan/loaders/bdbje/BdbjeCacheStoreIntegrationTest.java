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
package org.infinispan.loaders.bdbje;

import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Adrian Cole
 * @since 4.0
 */
@Test(groups = "unit", enabled = true, testName = "loaders.bdbje.BdbjeCacheStoreIntegrationTest")
public class BdbjeCacheStoreIntegrationTest extends BaseCacheStoreTest {

   private String tmpDirectory;
   private TransactionFactory gts = new TransactionFactory();

   public BdbjeCacheStoreIntegrationTest() {
      gts.init(false, false, true, false);
   }

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   protected CacheStore createCacheStore() throws CacheLoaderException {
//      clearTempDir();
      CacheStore cs = new BdbjeCacheStore();
      BdbjeCacheStoreConfig cfg = new BdbjeCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true);
      cs.init(cfg, getCache(), getMarshaller());
      cs.start();
      return cs;
   }

   /**
    * this is the same as the superclass, except that it doesn't attempt read-committed
    */
   @Override
   public void testTwoPhaseCommit() throws CacheLoaderException {
      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      GlobalTransaction tx = gts.newGlobalTransaction(null, false);
      cs.prepare(mods, tx, false);
      cs.commit(tx);

      assert cs.load("k2").getValue().equals("v2");
      assert !cs.containsKey("k1");

      cs.clear();

      mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Clear());
      mods.add(new Store(TestInternalCacheEntryFactory.create("k3", "v3")));

      cs.prepare(mods, tx, false);
      cs.commit(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert cs.containsKey("k3");
   }

   /**
    * this is the same as the superclass, except that it doesn't attempt read-committed
    */
   @Override
   public void testRollback() throws CacheLoaderException {

      cs.store(TestInternalCacheEntryFactory.create("old", "old"));

      List<Modification> mods = new ArrayList<Modification>();
      mods.add(new Store(TestInternalCacheEntryFactory.create("k1", "v1")));
      mods.add(new Store(TestInternalCacheEntryFactory.create("k2", "v2")));
      mods.add(new Remove("k1"));
      mods.add(new Remove("old"));
      GlobalTransaction tx = gts.newGlobalTransaction(null, false);
      cs.prepare(mods, tx, false);
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
      cs.rollback(tx);

      assert !cs.containsKey("k1");
      assert !cs.containsKey("k2");
      assert !cs.containsKey("k3");
      assert cs.containsKey("old");
   }

}
