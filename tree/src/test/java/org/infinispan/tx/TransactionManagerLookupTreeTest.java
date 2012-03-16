/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
import org.testng.annotations.Test;

@Test(testName = "tx.TransactionManagerLookupTreeTest", groups = "unit")
public class TransactionManagerLookupTreeTest extends TransactionManagerLookupTest {

   @Override
   protected void doTest(TransactionManagerLookup tml) {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setTransactionManagerLookup(tml);
         c.setInvocationBatchingEnabled(true);
         ecm = new DefaultCacheManager(c);
         TreeCache<Object, Object> tc = new TreeCacheFactory().createTreeCache(ecm.<Object, Object>getCache());
         tc.put("/a/b/c", "k", "v");
         assert "v".equals(tc.get("/a/b/c", "k"));
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }
}
