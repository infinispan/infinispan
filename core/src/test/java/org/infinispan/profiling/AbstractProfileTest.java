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
package org.infinispan.profiling;

import static org.infinispan.config.Configuration.CacheMode.DIST_ASYNC;
import static org.infinispan.config.Configuration.CacheMode.DIST_SYNC;
import static org.infinispan.config.Configuration.CacheMode.REPL_ASYNC;
import static org.infinispan.config.Configuration.CacheMode.REPL_SYNC;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.infinispan.api.executors.ExecutorFactory;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

@Test(groups = "profiling", enabled = false, testName = "profiling.AbstractProfileTest")
public abstract class AbstractProfileTest extends SingleCacheManagerTest {

   protected static final String LOCAL_CACHE_NAME = "local";
   protected static final String REPL_SYNC_CACHE_NAME = "repl_sync";
   protected static final String REPL_ASYNC_CACHE_NAME = "repl_async";
   protected static final String DIST_SYNC_L1_CACHE_NAME = "dist_sync_l1";
   protected static final String DIST_ASYNC_L1_CACHE_NAME = "dist_async_l1";
   protected static final String DIST_SYNC_CACHE_NAME = "dist_sync";
   protected static final String DIST_ASYNC_CACHE_NAME = "dist_async";

   boolean startedInCmdLine = false;
   String clusterNameOverride = null;

   protected void initTest() throws Exception {
      System.out.println("Setting up test params!");
      if (startedInCmdLine) cacheManager = createCacheManager();
   }

   private Configuration getBaseCfg() {
      Configuration cfg = new Configuration();
      cfg.setConcurrencyLevel(5000);
      cfg.setTransactionManagerLookupClass(JBossStandaloneJTAManagerLookup.class.getName());
      return cfg;
   }

   private Configuration getClusteredCfg(Configuration.CacheMode mode, boolean l1) {
      Configuration cfg = getBaseCfg();
      cfg.setLockAcquisitionTimeout(60000);
      cfg.setSyncReplTimeout(60000);
      cfg.setCacheMode(mode);
      cfg.setFetchInMemoryState(false);
      if (mode.isDistributed()) {
         cfg.setL1CacheEnabled(l1);
         cfg.setL1Lifespan(120000);
      }
      return cfg;
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
      gc.setAsyncTransportExecutorFactoryClass(WTE.class.getName());
      cacheManager = TestCacheManagerFactory.createCacheManager(gc);

      cacheManager.defineConfiguration(LOCAL_CACHE_NAME, getBaseCfg());

      cacheManager.defineConfiguration(REPL_SYNC_CACHE_NAME, getClusteredCfg(REPL_SYNC, false));
      cacheManager.defineConfiguration(REPL_ASYNC_CACHE_NAME, getClusteredCfg(REPL_ASYNC, false));
      cacheManager.defineConfiguration(DIST_SYNC_CACHE_NAME, getClusteredCfg(DIST_SYNC, false));
      cacheManager.defineConfiguration(DIST_ASYNC_CACHE_NAME, getClusteredCfg(DIST_ASYNC, false));
      cacheManager.defineConfiguration(DIST_SYNC_L1_CACHE_NAME, getClusteredCfg(DIST_SYNC, true));
      cacheManager.defineConfiguration(DIST_ASYNC_L1_CACHE_NAME, getClusteredCfg(DIST_ASYNC, true));

      return cacheManager;
   }

   public static class WTE implements ExecutorFactory {
      public ExecutorService getExecutor(Properties p) {
         return new WithinThreadExecutor();
      }
   }
}
