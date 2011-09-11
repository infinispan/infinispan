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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.Set;

/**
 * Base class for {@link org.infinispan.test.SingleCacheManagerTest} and {@link org.infinispan.test.MultipleCacheManagersTest}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class AbstractCacheTest extends AbstractInfinispanTest {

   protected final Log log = LogFactory.getLog(getClass());

   public static enum CleanupPhase {
      AFTER_METHOD, AFTER_TEST
   }

   protected CleanupPhase cleanup = CleanupPhase.AFTER_TEST;

   protected boolean cleanupAfterTest() {
      return getClass().getAnnotation(CleanupAfterTest.class) != null || (
              getClass().getAnnotation(CleanupAfterMethod.class) == null &&
                      cleanup == CleanupPhase.AFTER_TEST
      );
   }

   protected boolean cleanupAfterMethod() {
      return getClass().getAnnotation(CleanupAfterMethod.class) != null || (
              getClass().getAnnotation(CleanupAfterTest.class) == null &&
                      cleanup == CleanupPhase.AFTER_METHOD
      );
   }


   /**
    * use TestingUtil.clearContent(cacheManager);
    */
   @Deprecated
   public void clearContent(EmbeddedCacheManager embeddedCacheManager) {
      TestingUtil.clearContent(embeddedCacheManager);
   }

   /**
    * use TestingUtil.getRunningCaches(cacheManager);
    */
   @Deprecated
   protected Set<Cache> getRunningCaches(EmbeddedCacheManager embeddedCacheManager) {
      return TestingUtil.getRunningCaches(embeddedCacheManager);
   }

   /**
    * When multiple test methods operate on same cluster, sync commit and rollback are mandatory. This is in order to
    * make sure that an commit message will be dispatched in the same test method it was triggered and it will not
    * interfere with further log messages.  This is a non-transactional configuration.
    */
   public static Configuration getDefaultClusteredConfig(Configuration.CacheMode mode) {
      return getDefaultClusteredConfig(mode, false);
   }

   public static Configuration getDefaultClusteredConfig(Configuration.CacheMode mode, boolean transactional) {
      Configuration configuration = TestCacheManagerFactory.getDefaultConfiguration(transactional);
      configuration.fluent().transaction().cacheStopTimeout(0);
      configuration.setCacheMode(mode);
      configuration.setSyncCommitPhase(true);
      configuration.setSyncRollbackPhase(true);
      configuration.setFetchInMemoryState(false);
      return configuration;
   }

   protected boolean xor(boolean b1, boolean b2) {
      return (b1 || b2) && !(b1 && b2);
   }

   protected void assertNotLocked(Cache cache, Object key) {
      LockManager lockManager = TestingUtil.extractLockManager(cache);
      assert !lockManager.isLocked(key) : "expected key '" + key + "' not to be locked, and it is by: " + lockManager.getOwner(key);
   }

   protected void assertLocked(Cache cache, Object key) {
      LockManager lockManager = TestingUtil.extractLockManager(cache);
      assert lockManager.isLocked(key) : "expected key '" + key + "' to be locked, but it is not";
   }

   public EmbeddedCacheManager manager(Cache c) {
      return c.getCacheManager();
   }
}
