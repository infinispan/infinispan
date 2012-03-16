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
package org.infinispan.jmx;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

import static org.infinispan.test.TestingUtil.getCacheObjectName;

/**
 * Test the JMX functionality in {@link org.infinispan.util.concurrent.locks.LockManagerImpl}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "jmx.MvccLockManagerMBeanTest")
public class MvccLockManagerMBeanTest extends SingleCacheManagerTest {
   public static final int CONCURRENCY_LEVEL = 129;

   private ObjectName lockManagerObjName;
   private MBeanServer threadMBeanServer;
   private static final String JMX_DOMAIN = "MvccLockManagerMBeanTest";

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault().fluent()
            .globalJmxStatistics()
               .mBeanServerLookupClass(PerThreadMBeanServerLookup.class)
               .jmxDomain(JMX_DOMAIN)
            .build();

      cacheManager = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);

      Configuration configuration = getDefaultStandaloneConfig(true).fluent()
            .jmxStatistics()
            .locking()
               .concurrencyLevel(CONCURRENCY_LEVEL)
               .useLockStriping(true)
            .transaction()
               .transactionManagerLookup(new DummyTransactionManagerLookup())
            .build();

      cacheManager.defineConfiguration("test", configuration);
      cache = cacheManager.getCache("test");
      lockManagerObjName = getCacheObjectName(JMX_DOMAIN, "test(local)", "LockManager");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      return cacheManager;
   }

   public void testConcurrencyLevel() throws Exception {
      assertAttributeValue("ConcurrencyLevel", CONCURRENCY_LEVEL);
   }

   public void testNumberOfLocksHeld() throws Exception {
      DummyTransactionManager tm = (DummyTransactionManager) TestingUtil.extractComponent(cache, TransactionManager.class);
      tm.begin();
      cache.put("key", "value");
      tm.getTransaction().runPrepare();
      assertAttributeValue("NumberOfLocksHeld", 1);
      tm.getTransaction().runCommitTx();
      assertAttributeValue("NumberOfLocksHeld", 0);
   }

   public void testNumberOfLocksAvailable() throws Exception {
      DummyTransactionManager tm = (DummyTransactionManager) TestingUtil.extractComponent(cache, TransactionManager.class);
      int initialAvailable = getAttrValue("NumberOfLocksAvailable");
      tm.begin();
      cache.put("key", "value");
      tm.getTransaction().runPrepare();

      assertAttributeValue("NumberOfLocksHeld", 1);
      assertAttributeValue("NumberOfLocksAvailable", initialAvailable - 1);
      tm.rollback();
      assertAttributeValue("NumberOfLocksAvailable", initialAvailable);
      assertAttributeValue("NumberOfLocksHeld", 0);
   }

   private void assertAttributeValue(String attrName, int expectedVal) throws Exception {
      int cl = getAttrValue(attrName);
      assert cl == expectedVal : "expected " + expectedVal + ", but received " + cl;
   }

   private int getAttrValue(String attrName) throws Exception {
      return Integer.parseInt(threadMBeanServer.getAttribute(lockManagerObjName, attrName).toString());
   }
}
