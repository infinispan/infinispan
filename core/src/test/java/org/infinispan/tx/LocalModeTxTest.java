/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.LocalModeTxTest")
public class LocalModeTxTest extends SingleCacheManagerTest {

   protected CacheManager createCacheManager() {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager(true);
      cache = cm.getCache();
      return cm;
   }

   public void testTxCommit1() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("key", "value");
      Transaction t = tm.suspend();
      assert cache.isEmpty();
      tm.resume(t);
      tm.commit();
      assert !cache.isEmpty();
   }

   public void testTxCommit3() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put("key", "value");
      tm.commit();
      assert !cache.isEmpty();
   }

   public void testNonTx() throws Exception {
      cache.put("key", "value");
      assert !cache.isEmpty();
   }

   public void testTxCommit2() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      cache.put("key", "old");
      tm.begin();
      assert cache.get("key").equals("old");
      cache.put("key", "value");
      assert cache.get("key").equals("value");
      Transaction t = tm.suspend();
      assert cache.get("key").equals("old");
      tm.resume(t);
      tm.commit();
      assert cache.get("key").equals("value");
      assert !cache.isEmpty();
   }

}
