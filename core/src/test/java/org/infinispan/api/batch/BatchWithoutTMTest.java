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
package org.infinispan.api.batch;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(groups = "functional", testName = "api.batch.BatchWithoutTMTest")
public class BatchWithoutTMTest extends AbstractBatchTest {

   EmbeddedCacheManager cm;

   @BeforeClass
   public void createCacheManager() {
      final Configuration defaultConfiguration = TestCacheManagerFactory.getDefaultConfiguration(true);
      defaultConfiguration.fluent().invocationBatching();
      defaultConfiguration.fluent().transaction().autoCommit(false);
      cm = TestCacheManagerFactory.createCacheManager(defaultConfiguration);
   }

   @AfterClass
   public void destroyCacheManager() {
      TestingUtil.killCacheManagers(cm);
      cm = null;
   }

   public void testBatchWithoutCfg() {
      Cache<String, String> cache = null;
      cache = createCache(false, "testBatchWithoutCfg");
      try {
         cache.startBatch();
         assert false : "Should have failed";
      }
      catch (ConfigurationException good) {
         // do nothing
      }

      try {
         cache.endBatch(true);
         assert false : "Should have failed";
      }
      catch (ConfigurationException good) {
         // do nothing
      }

      try {
         cache.endBatch(false);
         assert false : "Should have failed";
      }
      catch (ConfigurationException good) {
         // do nothing
      }
   }

   public void testEndBatchWithoutStartBatch() {
      Cache<String, String> cache = null;
      cache = createCache(true, "testEndBatchWithoutStartBatch");
      cache.endBatch(true);
      cache.endBatch(false);
      // should not fail.
   }

   public void testStartBatchIdempotency() {
      Cache<String, String> cache = null;
      cache = createCache(true, "testStartBatchIdempotency");
      cache.startBatch();
      cache.put("k", "v");
      cache.startBatch();     // again
      cache.put("k2", "v2");
      cache.endBatch(true);

      assert "v".equals(cache.get("k"));
      assert "v2".equals(cache.get("k2"));
   }


   private static final Log log = LogFactory.getLog(BatchWithoutTMTest.class);

   public void testBatchVisibility() throws InterruptedException {
      Cache<String, String> cache = null;
      cache = createCache(true, "testBatchVisibility");

      log.info("Here it starts...");

      cache.startBatch();
      cache.put("k", "v");
      assertEquals(getOnDifferentThread(cache, "k"), null , "Other thread should not see batch update till batch completes!");

      cache.endBatch(true);

      assertEquals("v", getOnDifferentThread(cache, "k"));
   }

   public void testBatchRollback() throws Exception {
      Cache<String, String> cache = null;
      cache = createCache(true, "testBatchRollback");
      cache.startBatch();
      cache.put("k", "v");
      cache.put("k2", "v2");

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;

      cache.endBatch(false);

      assert getOnDifferentThread(cache, "k") == null;
      assert getOnDifferentThread(cache, "k2") == null;
   }

   private Cache<String, String> createCache(boolean enableBatch, String name) {
      Configuration c = new Configuration();
      c.setInvocationBatchingEnabled(enableBatch);
      cm.defineConfiguration(name, c);
      return cm.getCache(name);
   }
}
