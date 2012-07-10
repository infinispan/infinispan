/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.lock;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

import static org.infinispan.test.TestingUtil.withTx;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.ExplicitLockingAndOptimisticCachesTest")
public class ExplicitLockingAndOptimisticCachesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final Configuration c = getDefaultStandaloneConfig(true);
      c.fluent().transaction().lockingMode(LockingMode.OPTIMISTIC);
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testExplicitLockingNotWorkingWithOptimisticCaches() throws Exception {
      // Also provide guarantees that the transaction will come to an end
      withTx(tm(), new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               cache.getAdvancedCache().lock("a");
               assert false;
            } catch (CacheException e) {
               // expected
            }
            return null;
         }
      });
   }

   public void testExplicitLockingOptimisticCachesFailSilent() throws Exception {
      // Also provide guarantees that the transaction will come to an end
      withTx(tm(), new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            try {
               cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY).lock("a");
               assert false : "Should be throwing an exception in spite of fail silent";
            } catch (CacheException e) {
               // expected
            }
            return null;
         }
      });
   }

}
