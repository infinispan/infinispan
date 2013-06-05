/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.api.batch;

import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertEquals;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(groups = {"functional", "transaction"}, testName = "api.batch.BatchWithTMTest")
public class BatchWithCustomTMTest extends AbstractBatchTest {

   EmbeddedCacheManager cm;

   @BeforeClass
   public void createCacheManager() {
      cm = TestCacheManagerFactory.createLocalCacheManager(false);
   }

   @AfterClass
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testBatchWithOngoingTM() throws Exception {
      Cache<String, String> cache = null;
      cache = createCache("testBatchWithOngoingTM");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assertEquals(MyDummyTransactionManager.class, tm.getClass());
      tm.begin();
      cache.put("k", "v");
      cache.startBatch();
      cache.put("k2", "v2");
      tm.commit();

      assertEquals("v", cache.get("k"));
      assertEquals("v2", cache.get("k2"));

      cache.endBatch(false); // should be a no op
      assertEquals("v", cache.get("k"));
      assertEquals("v2", cache.get("k2"));
   }

   public void testBatchWithoutOngoingTMSuspension() throws Exception {
      Cache<String, String> cache = createCache("testBatchWithoutOngoingTMSuspension");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assertEquals(MyDummyTransactionManager.class, tm.getClass());
      assertNull("Should have no ongoing txs", tm.getTransaction());
      cache.startBatch();

      cache.put("k", "v");
      assertNull("Should have no ongoing txs", tm.getTransaction());
      cache.put("k2", "v2");

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      try {
         tm.commit(); // should have no effect
      }
      catch (Exception e) {
         // the TM may barf here ... this is OK.
      }

      assertNull("Should have no ongoing txs", tm.getTransaction());

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      cache.endBatch(true); // should be a no op

      assertEquals("v", getOnDifferentThread(cache, "k"));
      assertEquals("v2", getOnDifferentThread(cache, "k2"));
   }

   public void testBatchRollback() throws Exception {
      Cache<String, String> cache = createCache("testBatchRollback");
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));

      cache.endBatch(false);

      assertNull(getOnDifferentThread(cache, "k"));
      assertNull(getOnDifferentThread(cache, "k2"));
   }

   private Cache<String, String> createCache(String name) {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.transaction().transactionManagerLookup(new MyDummyTransactionManagerLookup());
      c.invocationBatching().enable();
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      cm.defineConfiguration(name, c.build());
      return cm.getCache(name);
   }

   static class MyDummyTransactionManagerLookup extends DummyTransactionManagerLookup {
      MyDummyTransactionManager tm = new MyDummyTransactionManager();

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return tm;
      }
   }

   static class MyDummyTransactionManager extends DummyTransactionManager {

   }
}
