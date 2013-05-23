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
package org.infinispan.tx.exception;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.TransactionManager;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.ReplicationTxExceptionTest")
public class ReplicationTxExceptionTest extends MultipleCacheManagersTest {
   private ControlledRpcManager controlledRpcManager;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(config));
      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(config));
      TestingUtil.blockUntilViewsReceived(10000, cache(0), cache(1));
      Cache<?, ?> cache = cache(0);
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      controlledRpcManager = new ControlledRpcManager(rpcManager);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
   }

   public void testReplicationFailure() throws Exception {
      controlledRpcManager.failFor(PrepareCommand.class);
      try {
         TransactionManager tm = cache(0).getAdvancedCache().getTransactionManager();
         tm.begin();
         cache(0).put("k0", "v");
         try {
            tm.commit();
            assert false;
         } catch (RollbackException e) {
            //expected
         }
      } finally {
         controlledRpcManager.stopFailing();
      }
   }
}
