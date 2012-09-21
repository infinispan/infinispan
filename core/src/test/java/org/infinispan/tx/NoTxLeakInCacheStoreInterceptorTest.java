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

package org.infinispan.tx;

import static org.testng.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LoaderConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.interceptors.CacheStoreInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.testng.annotations.Test;

/**
 * Test for ISPN-2321: No GlobalTransaction objects must be left behind in 
 * CacheStoreInterceptor after one phase commit transactions. This test has 1PC transactions as the transactional
 * mode is pessimistic.
 *
 * @author Carsten Lohmann
 */
@Test(groups = "functional", testName = "tx.NoTxLeakInCacheStoreInterceptorTest")
public class NoTxLeakInCacheStoreInterceptorTest extends MultipleCacheManagersTest {

   private final String testCacheName = "testCache" + getClass().getSimpleName();
   
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().invocationBatching().enable().versioning().enable()
               .scheme(VersioningScheme.SIMPLE).transaction().lockingMode(LockingMode.PESSIMISTIC);
      cb.loaders().passivation(false).preload(true).shared(true);
      // Make it really shared by adding the test's name as store name
      LoaderConfigurationBuilder lb = cb.loaders().addCacheLoader()
               .cacheLoader(new DummyInMemoryCacheStore());
      lb.addProperty("storeName", getClass().getSimpleName());

      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager();
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager();
      registerCacheManager(cm1, cm2);
      
      defineConfigurationOnAllManagers(testCacheName, cb);
      waitForClusterToForm(testCacheName);
   }

   public void test() throws Exception {
      Cache<Object, Object> cache = cache(0, testCacheName);
      cache.put("k1", "v1");
      
      // ensure that the implicit transaction created for the "put" is cleaned up from the
      // CacheStoreInterceptor "preparingTxs" and "txStores" maps
      for (int i = 0; i < getCacheManagers().size(); i++) {
         CacheStoreInterceptor cacheStoreInterceptor = getCacheStoreInterceptor(i);
         assertTxFieldsEmpty(cacheStoreInterceptor, i);
      }
   }
   
   @SuppressWarnings("unchecked")
   private void assertTxFieldsEmpty(CacheStoreInterceptor cacheStoreInterceptor,
            int cacheManagerIndex) throws Exception {
      Map<GlobalTransaction, Set<Object>> preparingTxs = cacheStoreInterceptor.getPreparingTxs();
      log.debug("cm" + cacheManagerIndex + ": preparingTxs: " + preparingTxs);

      Map<GlobalTransaction, Integer> txStores = cacheStoreInterceptor.getTxStores();
      log.debug("cm" + cacheManagerIndex + ": txStores: " + txStores);

      assertTrue(preparingTxs.isEmpty());
      assertTrue(txStores.isEmpty());
   }

   private CacheStoreInterceptor getCacheStoreInterceptor(int cacheManagerIndex) {
      InterceptorChain interceptorChain = TestingUtil.extractComponentRegistry(
               cache(cacheManagerIndex, testCacheName)).getComponent(InterceptorChain.class);

      List<CommandInterceptor> interceptors = interceptorChain
               .getInterceptorsWhichExtend(CacheStoreInterceptor.class);
      if (interceptors.isEmpty()) {
         throw new IllegalStateException("no CacheStoreInterceptor found");
      }
      return (CacheStoreInterceptor) interceptors.get(0);
   }
}
