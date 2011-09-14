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
package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionTable;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

@Test(testName = "tx.ReadOnlyTxCleanupTest", groups = "functional")
@CleanupAfterMethod
public class ReadOnlyTxCleanupTest extends SingleCacheManagerTest {
   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testReadOnlyTx() throws SystemException, RollbackException, HeuristicRollbackException, HeuristicMixedException, NotSupportedException {
      Cache<String, String> c1 = cacheManager.getCache();
      Cache<String, String> c2 = cacheManager.getCache("two");

      c1.put("c1", "c1");
      c2.put("c2", "c2");

      TransactionManager tm1 = tm();
      tm1.begin();
      c1.get("c1");
      c2.get("c2");
      tm1.commit();

      TransactionTable tt1 = TestingUtil.extractComponent(c1, TransactionTable.class);
      TransactionTable tt2 = TestingUtil.extractComponent(c2, TransactionTable.class);

      assert tt1.getLocalTxCount() == 0;
      assert tt2.getLocalTxCount() == 0;
   }

}
