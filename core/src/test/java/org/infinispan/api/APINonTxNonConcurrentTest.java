/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.api;


import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.interceptors.locking.NonTransactionalLockingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "api.APINonTxNonConcurrentTest")
public class APINonTxNonConcurrentTest extends APINonTxTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder dscb = getDefaultStandaloneCacheConfig(false);
      dscb.locking().supportsConcurrentUpdates(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createLocalCacheManager(false);
      cm.defineConfiguration("test", dscb.build());
      cache = cm.getCache("test");
      return cm;
   }

   public void testNonLockingInterceptor() {
      assert !cache().getAdvancedCache().getInterceptorChain().contains(NonTransactionalLockingInterceptor.class);
   }
}
