/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301, USA.
 */

package org.infinispan.tx.totalorder.simple.repl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.OptimisticLockingInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedDistributionInterceptor;
import org.infinispan.interceptors.totalorder.TotalOrderVersionedEntryWrappingInterceptor;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

/**
 * @author Pedro Ruivo
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.simple.repl.FullSyncTotalOrderTest")
public class FullSyncWriteSkewUseSynchronizationTotalOrderTest extends FullAsyncTotalOrderTest {

   public FullSyncWriteSkewUseSynchronizationTotalOrderTest() {
      this(3);
   }

   public FullSyncWriteSkewUseSynchronizationTotalOrderTest(int clusterSize) {
      super(clusterSize, CacheMode.REPL_SYNC, true, true, true);
   }

   @Override
   public void testInterceptorChain() {
      InterceptorChain ic = advancedCache(0).getComponentRegistry().getComponent(InterceptorChain.class);
      assertTrue(ic.containsInterceptorType(TotalOrderInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedDistributionInterceptor.class));
      assertTrue(ic.containsInterceptorType(TotalOrderVersionedEntryWrappingInterceptor.class));
      assertFalse(ic.containsInterceptorType(OptimisticLockingInterceptor.class));
      assertFalse(ic.containsInterceptorType(PessimisticLockingInterceptor.class));
   }

   @Override
   public void testSinglePhaseTotalOrder() {
      assertFalse(Configurations.isOnePhaseTotalOrderCommit(cache(0).getCacheConfiguration()));
   }
}
