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
package org.infinispan.loaders;

import org.infinispan.config.Configuration;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import static org.infinispan.test.TestingUtil.getTransactionManager;

@Test(groups = "functional", testName = "loaders.ConcurrentLoadAndEvictTxTest")
public class ConcurrentLoadAndEvictTxTest extends SingleCacheManagerTest {

   TransactionManager tm;

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration config = new Configuration().fluent()
         .eviction().strategy(EvictionStrategy.FIFO).maxEntries(10)
         .expiration().wakeUpInterval(10L)
         .loaders().addCacheLoader(new DummyInMemoryCacheStore.Cfg())
         .build();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(config);
      cache = cm.getCache();
      tm = getTransactionManager(cache);
      return cm;
   }

   public void testEvictAndTx() throws SystemException, NotSupportedException, RollbackException, HeuristicRollbackException, HeuristicMixedException {
      for (int i=0; i<10; i++) {
         tm.begin();
         for (int j=0; j<10; j++) cache.put(String.format("key-%s-%s", i, j), "value");
         tm.commit();
         for (int j=0; j<10; j++) assert "value".equals(cache.get(String.format("key-%s-%s", i, j))) : "Data loss on key " + String.format("key-%s-%s", i, j);
      }
   }

}