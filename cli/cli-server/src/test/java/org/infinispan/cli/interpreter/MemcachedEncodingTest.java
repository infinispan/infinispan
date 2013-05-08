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
package org.infinispan.cli.interpreter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import net.spy.memcached.MemcachedClient;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.CacheValue;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.test.MemcachedTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
/**
 * MemcachedEncodingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(testName = "cli-server.MemcachedEncodingTest", groups = "functional")
@CleanupAfterMethod
public class MemcachedEncodingTest extends SingleCacheManagerTest {

   private static final String MEMCACHED_CACHE = "memcachedCache";
   MemcachedServer memcachedServer;
   int port;
   Interpreter interpreter;
   MemcachedClient memcachedClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      c.jmxStatistics().enable();
      cacheManager = TestCacheManagerFactory.createCacheManager(c);
      memcachedServer = MemcachedTestingUtil.startMemcachedTextServer(cacheManager);
      port = memcachedServer.getPort();
      memcachedClient = MemcachedTestingUtil.createMemcachedClient(60000, port);
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cacheManager);
      interpreter = gcr.getComponent(Interpreter.class);
      return cacheManager;
   }

   @AfterMethod
   public void release() {
      try {
         memcachedServer.stop();
         TestingUtil.killCacheManagers(cacheManager);
         memcachedClient.shutdown();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void testMemcachedCodec() throws Exception {
      Cache<ByteArrayKey, CacheValue> cache = cacheManager.getCache(MEMCACHED_CACHE);

      memcachedClient.set("k1", 3600, "v1").get();

      assertTrue(cache.containsKey("k1"));

      String sessionId = interpreter.createSessionId(MEMCACHED_CACHE);
      Map<String, String> response = interpreter.execute(sessionId, "get --codec=memcached k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "put --codec=memcached k2 v2;");
      String v2 = (String) memcachedClient.get("k2");
      assertEquals("v2", v2);
   }

   public void testMemcachedEncoding() throws Exception {
      Cache<ByteArrayKey, CacheValue> cache = cacheManager.getCache(MEMCACHED_CACHE);

      memcachedClient.set("k1", 3600, "v1").get();

      assertTrue(cache.containsKey("k1"));

      String sessionId = interpreter.createSessionId(MEMCACHED_CACHE);
      interpreter.execute(sessionId, "encoding memcached;");
      Map<String, String> response = interpreter.execute(sessionId, "get k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "put k2 v2;");
      String v2 = (String) memcachedClient.get("k2");
      assertEquals("v2", v2);
   }
}
