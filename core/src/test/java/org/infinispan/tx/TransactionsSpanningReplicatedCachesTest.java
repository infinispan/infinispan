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
package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManagerImpl;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

@Test(groups = "functional", testName = "tx.TransactionsSpanningReplicatedCachesTest")
public class TransactionsSpanningReplicatedCachesTest extends MultipleCacheManagersTest {

   private EmbeddedCacheManager cm1, cm2;

   public TransactionsSpanningReplicatedCachesTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Exception {
      ConfigurationBuilder c = getConfiguration();
      cm1 = addClusterEnabledCacheManager(c);
      cm2 = addClusterEnabledCacheManager(c);

      defineConfigurationOnAllManagers("c1", c);
      defineConfigurationOnAllManagers("c2", c);

      waitForClusterToForm();
   }

   private void startAllCaches() {
      startCache("c1");
      startCache("c2");
      startCache("cache1");
      startCache("cache2");
      startCache(CacheContainer.DEFAULT_CACHE_NAME);
   }

   private void startCache(String c1) {
      cm1.getCache(c1);
      cm2.getCache(c1);
      waitForClusterToForm(c1);
   }

   protected ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      c.jmxStatistics().enable();
      return c;
   }

   public void testReadOnlyTransaction() throws Exception {
      Cache<String, String> c1 = cm1.getCache();
      Cache<String, String> c2 = cm2.getCache();
      RpcManagerImpl ri = (RpcManagerImpl) c1.getAdvancedCache().getRpcManager();

      c1.put("k", "v");

      assert "v".equals(c1.get("k"));
      assert "v".equals(c2.get("k"));
      long oldRC = ri.getReplicationCount();
      c1.getAdvancedCache().getTransactionManager().begin();
      assert "v".equals(c1.get("k"));
      c1.getAdvancedCache().getTransactionManager().commit();

      assert ri.getReplicationCount() == oldRC;
   }

   public void testCommitSpanningCaches() throws Exception {
      startAllCaches();
      Cache c1 = cm1.getCache("c1");
      Cache c1Replica = cm2.getCache("c1");
      Cache c2 = cm1.getCache("c2");
      Cache c2Replica = cm2.getCache("c2");

      assert c1.isEmpty();
      assert c2.isEmpty();
      assert c1Replica.isEmpty();
      assert c2Replica.isEmpty();

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      assertInitialValues(c1, c1Replica, c2, c2Replica);

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assert c1.get("c1key").equals("c1value_new");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value_new");
      assert c2Replica.get("c2key").equals("c2value");

      Transaction tx = tm.suspend();


      assertInitialValues(c1, c1Replica, c2, c2Replica);

      tm.resume(tx);
      log.trace("before commit...");
      tm.commit();


      assert c1.get("c1key").equals("c1value_new");
      assert c1Replica.get("c1key").equals("c1value_new");
      assertEquals(c2.get("c2key"), "c2value_new");
      assert c2Replica.get("c2key").equals("c2value_new");
   }

   public void testRollbackSpanningCaches() throws Exception {
      startAllCaches();
      Cache c1 = cm1.getCache("c1");
      Cache c1Replica = cm2.getCache("c1");
      Cache c2 = cm1.getCache("c2");
      Cache c2Replica = cm2.getCache("c2");

      assert c1.isEmpty();
      assert c2.isEmpty();
      assert c1Replica.isEmpty();
      assert c2Replica.isEmpty();

      c1.put("c1key", "c1value");
      c2.put("c2key", "c2value");

      assertInitialValues(c1, c1Replica, c2, c2Replica);

      TransactionManager tm = TestingUtil.getTransactionManager(c1);

      tm.begin();
      c1.put("c1key", "c1value_new");
      c2.put("c2key", "c2value_new");

      assert c1.get("c1key").equals("c1value_new");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value_new");
      assert c2Replica.get("c2key").equals("c2value");

      Transaction tx = tm.suspend();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");
      assert c2Replica.get("c2key").equals("c2value");

      tm.resume(tx);
      tm.rollback();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
      assert c2.get("c2key").equals("c2value");
      assert c2Replica.get("c2key").equals("c2value");
   }

   private void assertInitialValues(Cache c1, Cache c1Replica, Cache c2, Cache c2Replica) {
      for (Cache c : Arrays.asList(c1, c1Replica)) {
         assert !c.isEmpty();
         assert c.size() == 1;
         assert c.get("c1key").equals("c1value");
      }

      for (Cache c : Arrays.asList(c2, c2Replica)) {
         assert !c.isEmpty();
         assert c.size() == 1;
         assert c.get("c2key").equals("c2value");
      }
   }

   public void testRollbackSpanningCaches2() throws Exception {
      startAllCaches();
      Cache c1 = cm1.getCache("c1");

      assert c1.getConfiguration().getCacheMode().isClustered();
      Cache c1Replica = cm2.getCache("c1");

      assert c1.isEmpty();
      assert c1Replica.isEmpty();

      c1.put("c1key", "c1value");
      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
   }

   public void testSimpleCommit() throws Exception {
      startAllCaches();
      Cache c1 = cm1.getCache("c1");
      Cache c1Replica = cm2.getCache("c1");


      assert c1.isEmpty();
      assert c1Replica.isEmpty();

      TransactionManager tm = TestingUtil.getTransactionManager(c1);
      tm.begin();
      c1.put("c1key", "c1value");
      tm.commit();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
   }

   public void testPutIfAbsent() throws Exception {
      startAllCaches();
      Cache c1 = cm1.getCache("c1");
      Cache c1Replica = cm2.getCache("c1");


      assert c1.isEmpty();
      assert c1Replica.isEmpty();

      TransactionManager tm = TestingUtil.getTransactionManager(c1);
      tm.begin();
      c1.put("c1key", "c1value");
      tm.commit();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");

      tm.begin();
      c1.putIfAbsent("c1key", "SHOULD_NOT_GET_INSERTED");
      tm.commit();

      assert c1.get("c1key").equals("c1value");
      assert c1Replica.get("c1key").equals("c1value");
   }

   public void testTwoNamedCachesSameNode() throws Exception {
      runTest(cache(0, "cache1"), cache(0, "cache2"));
   }

   public void testDefaultCacheAndNamedCacheSameNode() throws Exception {
      runTest(cache(0), cache(0, "cache1"));
   }

   public void testTwoNamedCachesDifferentNodes() throws Exception {
      runTest(cache(0, "cache1"), cache(1, "cache2"));
   }

   public void testDefaultCacheAndNamedCacheDifferentNodes() throws Exception {
      runTest(cache(0), cache(1, "cache1"));
   }

   private void runTest(Cache cache1, Cache cache2) throws Exception {
      startAllCaches();
      assertFalse(cache1.containsKey("a"));
      assertFalse(cache2.containsKey("b"));

      TransactionManager tm = TestingUtil.getTransactionManager(cache1);
      tm.begin();
      cache1.put("a", "value1");
      cache2.put("b", "value2");
      tm.commit();

      assertEquals("value1", cache1.get("a"));
      assertEquals("value2", cache2.get("b"));

      tm.begin();
      cache1.remove("a");
      cache2.remove("b");
      tm.commit();

      assertFalse(cache1.containsKey("a"));
      assertFalse(cache2.containsKey("b"));
   }

}