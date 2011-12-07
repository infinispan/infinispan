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

/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.infinispan.api.tree;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheImpl;
import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

/**
 * @author <a href="mailto:manik AT jboss DOT org">Manik Surtani (manik AT jboss DOT org)</a>
 */
@Test(groups = "functional", testName = "api.tree.SyncReplTxTest")
public class SyncReplTxTest extends MultipleCacheManagersTest {
   TreeCache<Object, Object> cache1, cache2;

   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      c.setInvocationBatchingEnabled(true);

      createClusteredCaches(2, "replSync", c);

      Cache c1 = cache(0, "replSync");
      Cache c2 = cache(1, "replSync");

      cache1 = new TreeCacheImpl<Object, Object>(c1);
      cache2 = new TreeCacheImpl<Object, Object>(c2);
   }

   private TransactionManager beginTransaction(Cache cache) throws NotSupportedException, SystemException {
      TransactionManager mgr = cache.getAdvancedCache().getTransactionManager();
      mgr.begin();
      return mgr;
   }

   public void testBasicOperation() throws SystemException, NotSupportedException, HeuristicMixedException, HeuristicRollbackException, RollbackException {
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      Fqn f = Fqn.fromString("/test/data");
      String k = "key", v = "value";

      assertNull("Should be null", cache1.getRoot().getChild(f));
      assertNull("Should be null", cache2.getRoot().getChild(f));

      Node<Object, Object> node = cache1.getRoot().addChild(f);

      assertNotNull("Should not be null", node);

      TransactionManager tm = beginTransaction(cache1.getCache());
      node.put(k, v);
      tm.commit();

      assertEquals(v, node.get(k));
      assertEquals(v, cache1.get(f, k));
      assertEquals("Should have replicated", v, cache2.get(f, k));
   }
}