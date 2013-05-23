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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;

import java.util.Set;

/**
 * Base class for {@link org.infinispan.test.SingleCacheManagerTest} and {@link org.infinispan.test.MultipleCacheManagersTest}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class AbstractCacheTest extends AbstractInfinispanTest {

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
      if (mode.isSynchronous()) {
         return TestCacheManagerFactory.getDefaultConfiguration(transactional).fluent()
            .mode(mode)
            .clustering()
               .sync()
                  .stateRetrieval().fetchInMemoryState(false)
               .transaction().syncCommitPhase(true).syncRollbackPhase(true)
               .cacheStopTimeout(0)
            .build();
      } else {
         return TestCacheManagerFactory.getDefaultConfiguration(transactional).fluent()
            .mode(mode)
            .clustering()
               .async()
                  .stateRetrieval().fetchInMemoryState(false)
               .transaction().syncCommitPhase(true).syncRollbackPhase(true)
               .cacheStopTimeout(0)
            .build();
      }
   }

   public static ConfigurationBuilder getDefaultClusteredCacheConfig(CacheMode mode, boolean transactional) {
      return getDefaultClusteredCacheConfig(mode, transactional, false);
   }

   public static ConfigurationBuilder getDefaultClusteredCacheConfig(CacheMode mode, boolean transactional, boolean useCustomTxLookup) {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(transactional, useCustomTxLookup);
      builder.
         clustering()
            .cacheMode(mode)
            .stateTransfer().fetchInMemoryState(false)
         .transaction().syncCommitPhase(true).syncRollbackPhase(true)
         .cacheStopTimeout(0L);

      if (mode.isSynchronous())
         builder.clustering().sync();
      else
         builder.clustering().async();

      if (mode.isReplicated()) {
         // only one segment is supported for REPL tests now because some old tests still expect a single primary owner
         builder.clustering().hash().numSegments(1);
      }

      return builder;
   }

   protected boolean xor(boolean b1, boolean b2) {
      return (b1 || b2) && !(b1 && b2);
   }

   protected void assertNotLocked(final Cache cache, final Object key) {
      //lock release happens async, hence the eventually...
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(cache, key);
         }
      });
   }

   protected void assertLocked(Cache cache, Object key) {
      assert checkLocked(cache, key) : "expected key '" + key + "' to be locked on cache " + cache + ", but it is not";
   }

   protected boolean checkLocked(Cache cache, Object key) {
      LockManager lockManager = TestingUtil.extractLockManager(cache);
      return lockManager.isLocked(key);
   }

   public EmbeddedCacheManager manager(Cache c) {
      return c.getCacheManager();
   }
}
