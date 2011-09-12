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

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "tx.MarkAsRollbackTest")
public class MarkAsRollbackTest extends SingleCacheManagerTest {

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(new Configuration());
      cache = cm.getCache();
      return cm;
   }

   public void testMarkAsRollbackAfterMods() throws Exception {

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assert tm != null;
      tm.begin();
      cache.put("k", "v");
      assert cache.get("k").equals("v");
      tm.setRollbackOnly();
      try {
         tm.commit();
         assert false : "Should have rolled back";
      }
      catch (RollbackException expected) {
      }

      assert tm.getTransaction() == null : "There should be no transaction in scope anymore!";
      assert cache.get("k") == null : "Expected a null but was " + cache.get("k");
   }

   public void testMarkAsRollbackBeforeMods() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      assert tm != null;
      tm.begin();
      tm.setRollbackOnly();
      try {
         cache.put("k", "v");
         assert false : "Should have throw an illegal state exception";
      } catch (IllegalStateException expected) {

      }
      try {
         tm.commit();
         assert false : "Should have rolled back";
      }
      catch (RollbackException expected) {

      }

      assert tm.getTransaction() == null : "There should be no transaction in scope anymore!";
      assert cache.get("k") == null : "Expected a null but was " + cache.get("k");
   }
}
