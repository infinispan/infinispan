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

package org.infinispan.config;

import static org.testng.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.api.CacheException;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "config.TransactionalCacheConfigTest")
public class TransactionalCacheConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager(getDefaultStandaloneConfig(true));
   }

   public void test() {
      final Configuration c = TestCacheManagerFactory.getDefaultConfiguration(false);
      assert !c.isTransactionalCache();
      c.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      assert c.isTransactionalCache();
      c.fluent().transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      assert !c.isTransactionalCache();
   }

   public void testTransactionModeOverride() {
      Configuration c = new Configuration();
      c.fluent().transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      assertEquals(cacheManager.getCache().getConfiguration().getTransactionMode(), TransactionMode.TRANSACTIONAL);
      cacheManager.defineConfiguration("nonTx", c);
      assertEquals(cacheManager.getCache("nonTx").getConfiguration().getTransactionMode(), TransactionMode.NON_TRANSACTIONAL);
   }

   public void testDefaults() {
      Configuration c = new Configuration();
      assert !c.isTransactionalCache();
      assertTmLookupSet(c, false);

      c = TestCacheManagerFactory.getDefaultConfiguration(false);
      assert !c.isTransactionalCache();
      assertTmLookupSet(c, false);

      c = TestCacheManagerFactory.getDefaultConfiguration(true);
      assert c.isTransactionalCache();
      assertTmLookupSet(c, true);

      c = TestCacheManagerFactory.getDefaultConfiguration(false, Configuration.CacheMode.DIST_SYNC);
      assert !c.isTransactionalCache();
      assertTmLookupSet(c, false);

      c = TestCacheManagerFactory.getDefaultConfiguration(true, Configuration.CacheMode.DIST_SYNC);
      assert c.isTransactionalCache();
      assertTmLookupSet(c, true);
   }

   public void testTransactionalityInduced() {
      Configuration c = new Configuration();
      assert !c.isTransactionalCache();

      c.setTransactionManagerLookup(new DummyTransactionManagerLookup());
      assert c.isTransactionalCache();

      c = new Configuration();
      assert !c.isTransactionalCache();

      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      assert c.isTransactionalCache();

      c = new Configuration();
      assert !c.isTransactionalCache();

      c.fluent().transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      assert c.isTransactionalCache();

      c = new Configuration();
      assert !c.isTransactionalCache();

      c.fluent().transaction().transactionManagerLookupClass(DummyTransactionManagerLookup.class);
      assert c.isTransactionalCache();

      c = new Configuration();
      assert !c.isTransactionalCache();

      c.fluent().invocationBatching();
      assert c.isTransactionalCache();

      c = new Configuration();
      assert !c.isTransactionalCache();

      c.setInvocationBatchingEnabled(true);
      assert c.isTransactionalCache();
   }

   public void testTransactionalCacheWithoutTransactionManagerLookup() {
      Configuration c = new Configuration();
      assert !c.isTransactionalCache();
      c.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL);

      DefaultCacheManager dcm = new DefaultCacheManager(c);
      try {
         dcm.getCache();
         assert false : "This should not start as the cache doesn't have a TM configured.";
      } catch (CacheException e) {
         e.printStackTrace();
      }
   }

   public void testInvocationBatchingAndInducedTm() {
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);
      assert c.isTransactionalCache();
      DefaultCacheManager dcm = new DefaultCacheManager(c);
      assert  dcm.getCache().getAdvancedCache().getTransactionManager() != null;
   }

   public void testOverride() {
      Configuration c = new Configuration();
      c.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new DummyTransactionManagerLookup());

      DefaultCacheManager cm = new DefaultCacheManager();
      cm.defineConfiguration("transactional", c);
      Cache cache = cm.getCache("transactional");
      assert cache.getConfiguration().isTransactionalCache();
   }

   public void testBatchingAndTransactionalCache() {
      Configuration c = new Configuration();
      c.fluent().invocationBatching();

      assert c.isInvocationBatchingEnabled();
      assert c.isTransactionalCache();

      DefaultCacheManager dcm = new DefaultCacheManager();
      assert !dcm.getCache().getConfiguration().isTransactionalCache();

      dcm.defineConfiguration("a", c);
      final Cache<Object, Object> a = dcm.getCache("a");

      assert a.getConfiguration().isInvocationBatchingEnabled();
      assert a.getConfiguration().isTransactionalCache();
   }

   public void testBatchingAndTransactionalCache2() {
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(true);

      assert c.isInvocationBatchingEnabled();
      assert c.isTransactionalCache();

      DefaultCacheManager dcm = new DefaultCacheManager();
      assert !dcm.getCache().getConfiguration().isTransactionalCache();

      dcm.defineConfiguration("a", c);
      final Cache<Object, Object> a = dcm.getCache("a");

      assert a.getConfiguration().isInvocationBatchingEnabled();
      assert a.getConfiguration().isTransactionalCache();
   }


   private void assertTmLookupSet(Configuration c, boolean b) {
      assert b == (c.getTransactionManagerLookup() != null || c.getTransactionManagerLookupClass() != null);
   }
}
