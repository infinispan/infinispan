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
package org.infinispan.statetransfer;

import java.io.File;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.infinispan.Cache;
import org.infinispan.config.CacheLoaderManagerConfig;
import org.infinispan.config.Configuration;
import org.infinispan.loaders.file.FileCacheStoreConfig;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import static org.infinispan.statetransfer.StateTransferTestingUtil.*;

/**
 * StateTransferFileCacheStoreFunctionalTest.
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
@Test(groups = "functional", testName = "statetransfer.StateTransferFileCacheLoaderFunctionalTest")
public class StateTransferFileCacheLoaderFunctionalTest extends MultipleCacheManagersTest {
   static final Log log = LogFactory.getLog(StateTransferFileCacheLoaderFunctionalTest.class);
   static String cacheName = "nbst-with-file-loader";
   volatile int testCount = 0;
   ThreadLocal<Boolean> sharedCacheLoader = new ThreadLocal<Boolean>() {
      protected Boolean initialValue() {
         return false;
      }
   };
   String tmpDirectory1;
   String tmpDirectory2;
   String tmpDirectory3;
   String tmpDirectory4;

   Configuration config;

   @BeforeTest
   @Parameters({"basedir"})
   protected void setUpTempDir(@Optional(value = "/tmp") String basedir) {
      tmpDirectory1 = basedir + File.separator + TestingUtil.TEST_PATH + File.separator + "1" + File.separator + getClass().getSimpleName();
      tmpDirectory2 = basedir + File.separator + TestingUtil.TEST_PATH + File.separator + "2" + File.separator + getClass().getSimpleName();
      tmpDirectory3 = basedir + File.separator + TestingUtil.TEST_PATH + File.separator + "3" + File.separator + getClass().getSimpleName();
      tmpDirectory4 = basedir + File.separator + TestingUtil.TEST_PATH + File.separator + "4" + File.separator + getClass().getSimpleName();
   }

   @AfterMethod(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory1);
      new File(tmpDirectory1).mkdirs();
      TestingUtil.recursiveFileRemove(tmpDirectory2);
      new File(tmpDirectory2).mkdirs();
      TestingUtil.recursiveFileRemove(tmpDirectory3);
      new File(tmpDirectory3).mkdirs();
      TestingUtil.recursiveFileRemove(tmpDirectory4);
      new File(tmpDirectory4).mkdirs();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      // This impl only really sets up a configuration for use later.
      config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      config.setSyncReplTimeout(30000);
      config.setFetchInMemoryState(true);
      config.setUseLockStriping(false); // reduces the odd chance of a key collision and deadlock
   }

   protected CacheContainer createCacheManager(String tmpDirectory) {
      // increment the DIMCS store id
      FileCacheStoreConfig cfg = new FileCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      cfg.setFetchPersistentState(true);

      CacheLoaderManagerConfig clmc = new CacheLoaderManagerConfig();
      clmc.addCacheLoaderConfig(cfg);
      clmc.setShared(sharedCacheLoader.get());
      config.setCacheLoaderManagerConfig(clmc);

      EmbeddedCacheManager cm = addClusterEnabledCacheManager();
      cm.defineConfiguration(cacheName, config.clone());
      return cm;
   }

   public void testSharedLoader() throws Exception {
      CacheContainer cm1 = null, cm2 = null;
      try {
         sharedCacheLoader.set(true);
         cm1 = createCacheManager(tmpDirectory1);
         Cache c1 = cm1.getCache(cacheName);
         verifyNoDataOnLoader(c1);
         verifyNoData(c1);
         writeInitialData(c1);

         // starting the second cache would initialize an in-memory state transfer but not a persistent one since the loader is shared
         cm2 = createCacheManager(tmpDirectory2);
         Cache c2 = cm2.getCache(cacheName);

         TestingUtil.blockUntilViewsReceived(60000, c1, c2);

         verifyInitialDataOnLoader(c1);
         verifyInitialData(c1);

         verifyNoDataOnLoader(c2);
         verifyNoData(c2);
      } finally {
         if (cm1 != null) cm1.stop();
         if (cm2 != null) cm2.stop();
         sharedCacheLoader.set(false);
      }
   }

   public void testInitialStateTransfer() throws Exception {
      testCount++;
      log.info("testInitialStateTransfer start - " + testCount);
      CacheContainer cm1 = null, cm2 = null;
      try {
         Cache<Object, Object> cache1, cache2;
         cm1 = createCacheManager(tmpDirectory1);
         cache1 = cm1.getCache(cacheName);
         writeInitialData(cache1);

         cm2 = createCacheManager(tmpDirectory2);
         cache2 = cm2.getCache(cacheName);

         // Pause to give caches time to see each other
         TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);

         verifyInitialData(cache2);
         log.info("testInitialStateTransfer end - " + testCount);
      } finally {
         if (cm1 != null) cm1.stop();
         if (cm2 != null) cm2.stop();
      }
   }

   public void testInitialStateTransferInDifferentThread(Method m) throws Exception {
      testCount++;
      log.info(m.getName() + " start - " + testCount);
      CacheContainer cm1 = null, cm2 = null, cm30 = null;
      try {
         Cache<Object, Object> cache1 = null, cache2 = null, cache3 = null;
         cm1 = createCacheManager(tmpDirectory1);
         cache1 = cm1.getCache(cacheName);
         writeInitialData(cache1);

         cm2 = createCacheManager(tmpDirectory2);
         cache2 = cm2.getCache(cacheName);

         cache1.put("delay", new StateTransferFunctionalTest.DelayTransfer());

         // Pause to give caches time to see each other
         TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);
         verifyInitialData(cache2);

         final CacheContainer cm3 = createCacheManager(tmpDirectory3);

         cm30 = cm3;

         Future<Void> f1 = fork(new Callable<Void>() {
            public Void call() throws Exception {
               cm3.getCache(cacheName);
               return null;
            }
         });

         f1.get();

         cache3 = cm3.getCache(cacheName);

         TestingUtil.blockUntilViewsReceived(120000, cache1, cache2, cache3);
         verifyInitialData(cache3);
         log.info("testConcurrentStateTransfer end - " + testCount);
      } finally {
         if (cm1 != null) cm1.stop();
         if (cm2 != null) cm2.stop();
         if (cm30 != null) cm30.stop();
      }
   }

   public void testConcurrentStateTransfer() throws Exception {
      testCount++;
      log.info("testConcurrentStateTransfer start - " + testCount);
      CacheContainer cm1 = null, cm2 = null, cm30 = null, cm40 = null;
      try {
         Cache<Object, Object> cache1 = null, cache2 = null, cache3 = null, cache4 = null;
         cm1 = createCacheManager(tmpDirectory1);
         cache1 = cm1.getCache(cacheName);
         writeInitialData(cache1);

         cm2 = createCacheManager(tmpDirectory2);
         cache2 = cm2.getCache(cacheName);

         cache1.put("delay", new StateTransferFunctionalTest.DelayTransfer());

         // Pause to give caches time to see each other
         TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);
         verifyInitialData(cache2);

         final CacheContainer cm3 = createCacheManager(tmpDirectory3);
         final CacheContainer cm4 = createCacheManager(tmpDirectory4);

         cm30 = cm3;
         cm40 = cm4;

         Future<Void> f1 = fork(new Callable<Void>() {
            public Void call() throws Exception {
               cm3.getCache(cacheName);
               return null;
            }
         });

         Future<Void> f2 = fork(new Callable<Void>() {
            public Void call() throws Exception {
               cm4.getCache(cacheName);
               return null;
            }
         });

         f1.get();
         f2.get();

         cache3 = cm3.getCache(cacheName);
         cache4 = cm4.getCache(cacheName);

         TestingUtil.blockUntilViewsReceived(120000, cache1, cache2, cache3, cache4);
         verifyInitialData(cache3);
         verifyInitialData(cache4);
         log.info("testConcurrentStateTransfer end - " + testCount);
      } finally {
         if (cm1 != null) cm1.stop();
         if (cm2 != null) cm2.stop();
         if (cm30 != null) cm30.stop();
         if (cm40 != null) cm40.stop();
      }
   }


}
