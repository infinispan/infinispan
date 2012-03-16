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
package org.infinispan.config;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.infinispan.config.Configuration.CacheMode.*;

/**
 * ConfigurationValidationTest.
 *
 * @author Sanne Grinovero
 * @since 4.0
 */
@Test(groups = "functional", testName = "config.ConfigurationValidationTest")
public class ConfigurationValidationTest extends AbstractInfinispanTest {

   @Test(expectedExceptions = ConfigurationException.class)
   public void testWrongCacheModeConfiguration() throws Exception {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = createCacheManager();
         cacheManager.getCache().put("key", "value");
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   public void testCacheModeConfiguration() throws Exception {
      EmbeddedCacheManager cacheManager = null;
      try {
         cacheManager = createCacheManager();
         cacheManager.getCache("local").put("key", "value");
      } finally {
         TestingUtil.killCacheManagers(cacheManager);
      }
   }

   @Test (expectedExceptions = ConfigurationException.class)
   public void testDistAndReplQueue() {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setCacheMode(DIST_SYNC);
         c.setUseReplQueue(true);
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   @Test (expectedExceptions = ConfigurationException.class)
   public void testSyncAndReplQueue() {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setCacheMode(REPL_SYNC);
         c.setUseReplQueue(true);
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }
   
   @Test (expectedExceptions = ConfigurationException.class)
   public void testEvictionOnButWithoutMaxEntries() {
      EmbeddedCacheManager ecm = null;
      try {
         Configuration c = new Configuration();
         c.setEvictionStrategy("LRU");
         ecm = TestCacheManagerFactory.createClusteredCacheManager(c);
         ecm.getCache();
      } finally {
         TestingUtil.killCacheManagers(ecm);
      }
   }

   private EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration gc = GlobalConfiguration.getNonClusteredDefault();
      Configuration config = new Configuration();
      config.setCacheMode(REPL_ASYNC);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gc, config);
      config = new Configuration();
      config.setCacheMode(LOCAL);
      cm.defineConfiguration("local", config);
      return cm;
   }
}
