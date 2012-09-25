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

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

@Test (groups = "functional", testName = "tx.DefaultEnlistmentModeTest")
public class DefaultEnlistmentModeTest extends AbstractCacheTest {

   private DefaultCacheManager dcm;

   @AfterMethod
   protected void destroyCacheManager() {
      TestingUtil.killCacheManagers(dcm);
   }

   public void testDefaultEnlistment() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      dcm = new DefaultCacheManager(getGlobalConfig(), builder.build());
      Cache<Object,Object> cache = dcm.getCache();
      assertTrue(cache.getCacheConfiguration().transaction().useSynchronization());
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
   }

   public void testXAEnlistment() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(false);
      dcm = new DefaultCacheManager(getGlobalConfig(), builder.build());
      Cache<Object,Object> cache = dcm.getCache();
      assertFalse(cache.getCacheConfiguration().transaction().useSynchronization());
      assertTrue(cache.getCacheConfiguration().transaction().recovery().enabled());
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
   }

   public void testXAEnlistmentNoRecovery() {
      ConfigurationBuilder builder = getLocalBuilder();
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(false)
            .recovery().disable();
      dcm = new DefaultCacheManager(getGlobalConfig(), builder.build());
      Cache<Object,Object> cache = dcm.getCache();
      assertFalse(cache.getCacheConfiguration().transaction().useSynchronization());
      assertFalse(cache.getCacheConfiguration().transaction().recovery().enabled());
   }

   private ConfigurationBuilder getLocalBuilder() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.clustering().cacheMode(CacheMode.LOCAL);
      return builder;
   }

   private GlobalConfiguration getGlobalConfig() {
      return new GlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true).build();
   }
}
