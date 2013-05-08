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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.rehash.XAResourceAdapter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

import static org.infinispan.test.TestingUtil.checkMBeanOperationParameterNaming;
import static org.infinispan.test.TestingUtil.getCacheObjectName;

@Test(groups = "functional", testName = "jmx.TxInterceptorMBeanTest")
public class TxInterceptorMBeanTest extends MultipleCacheManagersTest {
   private static final String JMX_DOMAIN = TxInterceptorMBeanTest.class.getSimpleName();

   private ObjectName txInterceptor;
   private ObjectName txInterceptor2;
   private MBeanServer threadMBeanServer;
   private TransactionManager tm;
   private Cache cache1;
   private Cache cache2;

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      globalConfiguration.setExposeGlobalJmxStatistics(true);
      globalConfiguration.setAllowDuplicateDomains(true);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setJmxDomain(JMX_DOMAIN);
      EmbeddedCacheManager cacheManager1 = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration);
      registerCacheManager(cacheManager1);
      GlobalConfiguration globalConfiguration2 = globalConfiguration.clone();
      globalConfiguration2.setCacheManagerName("SecondDefaultCacheManager");
      EmbeddedCacheManager cacheManager2 = TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(globalConfiguration2);
      registerCacheManager(cacheManager2);

      Configuration configuration = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      configuration.setExposeJmxStatistics(true);
      cacheManager1.defineConfiguration("test", configuration);
      cacheManager2.defineConfiguration("test", configuration.clone());
      cache1 = cacheManager1.getCache("test");
      cache2 = cacheManager2.getCache("test");
      txInterceptor = getCacheObjectName(JMX_DOMAIN, "test(repl_sync)", "Transactions");
      txInterceptor2 = getCacheObjectName(JMX_DOMAIN, "test(repl_sync)", "Transactions", "SecondDefaultCacheManager");

      threadMBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      tm = TestingUtil.getTransactionManager(cache1);
   }

   @AfterMethod
   public void resetStats() throws Exception {
      threadMBeanServer.invoke(txInterceptor, "resetStatistics", new Object[0], new String[0]);
      threadMBeanServer.invoke(txInterceptor2, "resetStatistics", new Object[0], new String[0]);
   }

   public void testJmxOperationMetadata() throws Exception {
      checkMBeanOperationParameterNaming(txInterceptor);
   }

   public void testCommit() throws Exception {
      assertCommitRollback(0, 0, txInterceptor);
      tm.begin();
      //enlist another resource adapter to force TM to execute 2PC (otherwise 1PC)
      tm.getTransaction().enlistResource(new XAResourceAdapter());
      assertCommitRollback(0, 0, txInterceptor);
      cache1.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor);
      tm.commit();
      assertCommitRollback(1, 0, txInterceptor);
   }

   public void testRollback() throws Exception {
      assertCommitRollback(0, 0, txInterceptor);
      tm.begin();
      assertCommitRollback(0, 0, txInterceptor);
      cache1.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor);
      tm.rollback();
      assertCommitRollback(0, 1, txInterceptor);
   }

   public void testRemoteCommit() throws Exception {
      assertCommitRollback(0, 0, txInterceptor2);
      tm.begin();
      assertCommitRollback(0, 0, txInterceptor2);
      //enlist another resource adapter to force TM to execute 2PC (otherwise 1PC)
      tm.getTransaction().enlistResource(new XAResourceAdapter());
      cache2.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor2);
      tm.commit();
      assertCommitRollback(1, 0, txInterceptor2);
   }

   public void testRemoteRollback() throws Exception {
      assertCommitRollback(0, 0, txInterceptor2);
      tm.begin();
      assertCommitRollback(0, 0, txInterceptor2);
      cache2.put("key", "value");
      assertCommitRollback(0, 0, txInterceptor2);
      tm.rollback();
      assertCommitRollback(0, 1, txInterceptor2);
   }

   private void assertCommitRollback(int commit, int rollback, ObjectName objectName) throws Exception {
      String commitCount = threadMBeanServer.getAttribute(objectName, "Commits").toString();
      assert Integer.valueOf(commitCount) == commit : "expecting " + commit + " commits, received " + commitCount;
      String rollbackCount = threadMBeanServer.getAttribute(objectName, "Rollbacks").toString();
      assert Integer.valueOf(commitCount) == commit : "expecting " + rollback + " rollbacks, received " + rollbackCount;
   }
}
