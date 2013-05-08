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
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.interceptors.BatchingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "manager.CacheManagerComponentRegistryTest")
public class CacheManagerComponentRegistryTest extends AbstractInfinispanTest {
   EmbeddedCacheManager cm;

   @AfterMethod
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testForceSharedComponents() {
      Configuration defaultCfg = new Configuration();
      defaultCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      defaultCfg.setFetchInMemoryState(false);
      defaultCfg.fluent().transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      defaultCfg.setFetchInMemoryState(false);

      // cache manager with default configuration
      cm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      Configuration overrides = TestCacheManagerFactory.getDefaultConfiguration(true);
      overrides.setTransactionManagerLookup(new DummyTransactionManagerLookup());
      cm.defineConfiguration("transactional", overrides);
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, TransactionManager.class) == null;
      assert TestingUtil.extractComponent(transactional, TransactionManager.class) instanceof DummyTransactionManager;

      // assert force-shared components
      assert TestingUtil.extractComponent(c, Transport.class) != null;
      assert TestingUtil.extractComponent(transactional, Transport.class) != null;
      assert TestingUtil.extractComponent(c, Transport.class) == TestingUtil.extractComponent(transactional, Transport.class);
   }

   public void testForceUnsharedComponents() {
      Configuration defaultCfg = new Configuration();
      defaultCfg.setFetchInMemoryState(false);
      defaultCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      defaultCfg.setEvictionStrategy(EvictionStrategy.NONE);
      // cache manager with default configuration
      cm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      Configuration overrides = new Configuration();
      overrides.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
      cm.defineConfiguration("transactional", overrides);
      Cache transactional = cm.getCache("transactional");

      // assert components.
      assert TestingUtil.extractComponent(c, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(transactional, EvictionManager.class) != null;
      assert TestingUtil.extractComponent(c, EvictionManager.class) != TestingUtil.extractComponent(transactional, EvictionManager.class);
   }

   public void testOverridingComponents() {
      Configuration defaultCfg = new Configuration();
      cm = TestCacheManagerFactory.createCacheManager(GlobalConfiguration.getClusteredDefault(), defaultCfg);

      // default cache with no overrides
      Cache c = cm.getCache();

      Configuration overrides = new Configuration();
      overrides.setInvocationBatchingEnabled(true);
      cm.defineConfiguration("overridden", overrides);
      Cache overridden = cm.getCache("overridden");

      // assert components.
      assert !TestingUtil.extractComponent(c, InterceptorChain.class).containsInterceptorType(BatchingInterceptor.class);
      assert TestingUtil.extractComponent(overridden, InterceptorChain.class).containsInterceptorType(BatchingInterceptor.class);
   }
}
