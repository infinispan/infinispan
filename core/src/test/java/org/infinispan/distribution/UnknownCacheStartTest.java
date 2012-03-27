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
package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.TestException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredConfig;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createCacheManager;

@Test(groups = "functional", testName = "distribution.UnknownCacheStartTest", enabled = false)
public class UnknownCacheStartTest extends AbstractInfinispanTest {

   Configuration configuration;
   EmbeddedCacheManager cm1, cm2;

   @BeforeTest
   public void setUp() {
      configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
   }

   @AfterTest
   public void tearDown() {
      killCacheManagers(cm1, cm2);
   }

   @Test (expectedExceptions = {CacheException.class, TestException.class}, timeOut = 60000, enabled = false)
   public void testStartingUnknownCaches() throws Throwable {
      TestCacheManagerFactory.backgroundTestStarted(this);
      try {
         cm1 = createCacheManager(configuration);

         cm1.defineConfiguration("new_1", configuration);

         Cache<String, String> c1 = cm1.getCache();
         Cache<String, String> c1_new = cm1.getCache("new_1");

         c1.put("k", "v");
         c1_new.put("k", "v");

         assert "v".equals(c1.get("k"));
         assert "v".equals(c1_new.get("k"));

         cm2 = createCacheManager(configuration);
         cm2.defineConfiguration("new_2", configuration);

         Cache<String, String> c2 = cm2.getCache();
         Cache<String, String> c2_new = cm2.getCache("new_AND_DEFINITELY_UNKNOWN_cache_2");

         c2.put("k", "v");
         c2_new.put("k", "v");

         assert "v".equals(c2.get("k"));
         assert "v".equals(c2_new.get("k"));

         TestingUtil.blockUntilViewsReceived(60000, false, c2, c2_new);
         TestingUtil.waitForRehashToComplete(c2, c2_new);

         assert false : "Should have thrown an exception!";
      } catch (CacheException expected) {
         // this is good
      }
   }
}
