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
package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransactionSetup;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

import static org.testng.Assert.assertEquals;


@Test(groups = {"functional", "transaction"}, testName = "api.batch.BatchWithTMTest")
public class BatchWithTMTest extends AbstractBatchTest {

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
      tm.begin();
      cache.put("k", "v");
      cache.startBatch();
      cache.put("k2", "v2");
      tm.commit();

      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));

      cache.endBatch(false); // should be a no op
      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));
   }

   public void testBatchWithoutOngoingTMSuspension() throws Exception {
      Cache cache = createCache("testBatchWithoutOngoingTMSuspension");
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assert tm.getTransaction() == null : "Should have no ongoing txs";
      cache.startBatch();
      cache.put("k", "v");
      assert tm.getTransaction() == null : "Should have no ongoing txs";
      cache.put("k2", "v2");

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;

      try {
         tm.commit(); // should have no effect
      }
      catch (Exception e) {
         // the TM may barf here ... this is OK.
      }

      assert tm.getTransaction() == null : "Should have no ongoing txs";

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;

      cache.endBatch(true); // should be a no op

      assert "v".equals(getOnDifferentThread(cache, "k"));
      assert "v2".equals(getOnDifferentThread(cache, "k2"));
   }

   public void testBatchRollback() throws Exception {
      Cache cache = createCache("testBatchRollback");
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assertEquals(getOnDifferentThread(cache, "k"), null);
      assert getOnDifferentThread(cache, "k2") == null;

      cache.endBatch(false);

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;
   }

   private Cache<String, String> createCache(String name) {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(TransactionSetup.getManagerLookup());
      c.setInvocationBatchingEnabled(true);
      c.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      assert c.getTransactionManagerLookupClass() != null : "Should have a transaction manager lookup class attached!!";
      cm.defineConfiguration(name, c);
      return cm.getCache(name);
   }
}
