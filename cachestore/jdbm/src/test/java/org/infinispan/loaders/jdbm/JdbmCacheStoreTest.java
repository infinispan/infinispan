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
package org.infinispan.loaders.jdbm;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.loaders.BaseCacheStoreTest;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbm.JdbmCacheStoreTest")
public class JdbmCacheStoreTest extends BaseCacheStoreTest {

   private JdbmCacheStore fcs;
   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   @Override
   protected CacheStore createCacheStore() throws CacheLoaderException {
      clearTempDir();
      fcs = new JdbmCacheStore();
      JdbmCacheStoreConfig cfg = new JdbmCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      fcs.init(cfg, getCache(), getMarshaller());
      fcs.start();
      return fcs;
   }

   @Override
   public void testPreload() throws Exception {
      super.testPreload();
   }

   @Override
   public void testPurgeExpired() throws Exception {
      long lifespan = 1000;
      InternalCacheEntry k1 = TestInternalCacheEntryFactory.create("k1", "v1", lifespan);
      InternalCacheEntry k2 = TestInternalCacheEntryFactory.create("k2", "v2", lifespan);
      InternalCacheEntry k3 = TestInternalCacheEntryFactory.create("k3", "v3", lifespan);
      cs.store(k1);
      cs.store(k2);
      cs.store(k3);
      assert cs.containsKey("k1");
      assert cs.containsKey("k2");
      assert cs.containsKey("k3");
      Thread.sleep(lifespan + 100);
      cs.purgeExpired();
      JdbmCacheStore fcs = (JdbmCacheStore) cs;
      assert fcs.load("k1") == null;
      assert fcs.load("k2") == null;
      assert fcs.load("k3") == null;
   }

   public void testStopStartDoesntNukeValues() throws InterruptedException, CacheLoaderException {
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
      Thread.sleep(100);
      // Force a purge expired so that expiry tree is updated
      cs.purgeExpired();
      cs.stop();
      cs.start();
      assert se1.isExpired(TIME_SERVICE.wallClockTime());
      assert cs.load("k1") == null;
      assert !cs.containsKey("k1");
      assert cs.load("k2") != null;
      assert cs.containsKey("k2");
      assert cs.load("k2").getValue().equals("v2");
      assert se3.isExpired(TIME_SERVICE.wallClockTime());
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
      assert se3.isExpired(TIME_SERVICE.wallClockTime());
      assert cs.load("k3") == null;
      assert !cs.containsKey("k3");
   }

   public void testIterator() throws Exception {
      InternalCacheEntry k1 = TestInternalCacheEntryFactory.create("k1", "v1");
      InternalCacheEntry k2 = TestInternalCacheEntryFactory.create("k2", "v2");
      cs.store(k1);
      cs.store(k2);

      Set<InternalCacheEntry> set = cs.loadAll();
      Iterator<InternalCacheEntry> i = set.iterator();
      assert i.hasNext() == true;
      assert i.hasNext() == true;
      assert i.next().getKey().equals("k1");
      assert i.next().getKey().equals("k2");
      assert i.hasNext() == false;
      assert i.hasNext() == false;
      try {
         i.next();
         assert false;
      } catch (NoSuchElementException e) {}
   }

}
