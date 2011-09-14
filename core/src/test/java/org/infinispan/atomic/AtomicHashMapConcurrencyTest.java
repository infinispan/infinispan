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
package org.infinispan.atomic;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.ValueFuture;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertEquals;

/**
 * Tester class for AtomicMapCache.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "atomic.AtomicHashMapConcurrencyTest")
public class AtomicHashMapConcurrencyTest extends AbstractInfinispanTest {

   private static final Log log = LogFactory.getLog(AtomicHashMapConcurrencyTest.class);

   public static final String KEY = "key";
   Cache<String, Object> cache;
   TransactionManager tm;
   private CacheContainer cm;

   @BeforeMethod
   @SuppressWarnings("unchecked")
   protected void setUp() {
      Configuration c = new Configuration();
      c.setLockAcquisitionTimeout(500);
      // these 2 need to be set to use the AtomicMapCache
      c.setInvocationBatchingEnabled(true);
      c.fluent()
            .transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC);

      cm = TestCacheManagerFactory.createCacheManager(c);
      cache = cm.getCache();
      tm = TestingUtil.getTransactionManager(cache);
   }

   @AfterMethod
   protected void tearDown() {
      try {
         tm.rollback();
      } catch (Exception e) {
      }
      TestingUtil.killCacheManagers(cm);
   }

   public void testConcurrentCreate() throws Exception {
      tm.begin();
      AtomicMapLookup.getAtomicMap(cache, KEY);

      final AtomicBoolean gotTimeoutException = new AtomicBoolean();
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm.begin();

               try {
                  AtomicMapLookup.getAtomicMap(cache, KEY);
               } catch (TimeoutException e) {
                  // this is the exception we were expecting
                  gotTimeoutException.set(true);
               } finally {
                  tm.rollback();
               }
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, true);

      assert gotTimeoutException.get();
   }

   public void testConcurrentModifications() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      tm.begin();
      atomicMap.put(1, "");

      final AtomicBoolean gotTimeoutException = new AtomicBoolean();
      fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm.begin();

               try {
                  AtomicMap<Integer, String> otMap = AtomicMapLookup.getAtomicMap(cache, KEY);
                  otMap.put(1, "val");
               } catch (TimeoutException e) {
                  // this is the exception we were expecting
                  gotTimeoutException.set(true);
               } finally {
                  tm.rollback();
               }
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, true);

      assert gotTimeoutException.get();
   }

   public void testReadAfterTxStarted() throws Exception {
      AtomicMap<Integer, String> atomicMap = AtomicMapLookup.getAtomicMap(cache, KEY);
      atomicMap.put(1, "existing");
      tm.begin();
      atomicMap.put(1, "newVal");

      final ValueFuture responseBeforeCommit = new ValueFuture();
      final ValueFuture responseAfterCommit = new ValueFuture();
      final CountDownLatch commitLatch = new CountDownLatch(1);

      Thread ot = fork(new Runnable() {
         @Override
         public void run() {
            try {
               tm.begin();

               try {
                  AtomicMap<Integer, String> otMap = AtomicMapLookup.getAtomicMap(cache, KEY);

                  responseBeforeCommit.set(otMap.get(1));

                  // wait until the main thread commits the transaction
                  commitLatch.await();

                  responseAfterCommit.set(otMap.get(1));
               } finally {
                  tm.rollback();
               }
            } catch (Exception e) {
               log.error("Unexpected error performing transaction", e);
            }
         }
      }, false);

      assertEquals(responseBeforeCommit.get(), "existing");

      tm.commit();
      commitLatch.countDown();

      assertEquals(atomicMap.get(1), "newVal");
      assertEquals(responseAfterCommit.get(), "newVal");
   }
}
