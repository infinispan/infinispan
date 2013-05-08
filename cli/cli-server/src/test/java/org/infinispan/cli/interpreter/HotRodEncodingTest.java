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

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.api.BasicCacheContainer;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * EncodingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(testName = "cli-server.HotRodEncodingTest", groups = "functional")
@CleanupAfterMethod
public class HotRodEncodingTest extends SingleCacheManagerTest {

   HotRodServer hotrodServer;
   int port;
   Interpreter interpreter;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      c.jmxStatistics().enable();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      port = hotrodServer.getPort();
      remoteCacheManager = new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder()
                  .addServers("localhost:" + hotrodServer.getPort()).build());
      remoteCacheManager.start();
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cacheManager);
      interpreter = gcr.getComponent(Interpreter.class);
   }

   @AfterTest
   public void release() {
      try {
         HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
         HotRodClientTestingUtil.killServers(hotrodServer);
         TestingUtil.killCacheManagers(cacheManager);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void testHotRodCodec() throws Exception {
      Cache<byte[], byte[]> cache = cacheManager.getCache();
      RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k1", "v1");
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      byte[] k1 = marshaller.objectToByteBuffer("k1");
      assertTrue(cache.containsKey(k1));

      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Map<String, String> response = interpreter.execute(sessionId, "get --codec=hotrod k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "put --codec=hotrod k2 v2;");
      String v2 = remoteCache.get("k2");
      assertEquals("v2", v2);
   }

   public void testHotRodEncoding() throws Exception {
      Cache<byte[], byte[]> cache = cacheManager.getCache();
      RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k1", "v1");
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      byte[] k1 = marshaller.objectToByteBuffer("k1");
      assertTrue(cache.containsKey(k1));

      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "encoding hotrod;");
      Map<String, String> response = interpreter.execute(sessionId, "get k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "put k2 v2;");
      String v2 = remoteCache.get("k2");
      assertEquals("v2", v2);
   }
}
