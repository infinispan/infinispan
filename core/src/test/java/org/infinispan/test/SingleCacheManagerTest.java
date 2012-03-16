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
package org.infinispan.test;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Base class for tests that operate on a single (most likely local) cache instance. This operates similar to {@link
 * org.infinispan.test.MultipleCacheManagersTest}, but on only once CacheManager.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.test.MultipleCacheManagersTest
 */
@Test
public abstract class SingleCacheManagerTest extends AbstractCacheTest {

   protected EmbeddedCacheManager cacheManager;
   protected Cache<Object, Object> cache;

   protected void setup() throws Exception {
      cacheManager = createCacheManager();
      if (cache == null) cache = cacheManager.getCache();
   }

   protected void teardown() {
      TestingUtil.killCacheManagers(cacheManager);
      cache = null;
      cacheManager = null;
   }

   @BeforeClass(alwaysRun = true)
   protected void createBeforeClass() throws Exception {
      try {
         if (cleanupAfterTest()) setup();
         else assert cleanupAfterMethod() : "you must either cleanup after test or after method";
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }

   @BeforeMethod(alwaysRun = true)
   protected void createBeforeMethod() throws Exception {
      try {
         if (cleanupAfterMethod()) setup();
         else assert cleanupAfterTest() : "you must either cleanup after test or after method";
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw e;
      }
   }
   
   @AfterClass(alwaysRun=true)
   protected void destroyAfterClass() {
      try {
         if (cleanupAfterTest()) teardown();
      } catch (Exception e) {
         log.error("Unexpected!", e);
      }
   }

   @AfterMethod(alwaysRun=true)
   protected void destroyAfterMethod() {
      if (cleanupAfterMethod()) teardown();
   }

   @AfterMethod(alwaysRun=true)
   protected void clearContent() {
      if (cleanupAfterTest()) TestingUtil.clearContent(cacheManager);
   }

   protected Configuration getDefaultStandaloneConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultConfiguration(transactional);
   }

   protected ConfigurationBuilder getDefaultStandaloneCacheConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultCacheConfiguration(transactional);
   }

   protected TransactionManager tm() {
      return cache.getAdvancedCache().getTransactionManager();
   }

   protected Transaction tx() {
      try {
         return cache.getAdvancedCache().getTransactionManager().getTransaction();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }

   protected LockManager lockManager(String cacheName) {
      return TestingUtil.extractLockManager(cacheManager.getCache(cacheName));
   }

   protected LockManager lockManager() {
      return TestingUtil.extractLockManager(cache);
   }


   protected abstract EmbeddedCacheManager createCacheManager() throws Exception;
}
