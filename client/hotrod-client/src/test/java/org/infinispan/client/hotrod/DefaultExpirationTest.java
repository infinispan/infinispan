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
package org.infinispan.client.hotrod;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.server.core.CacheValue;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayKey;
import org.testng.annotations.AfterClass;
import static org.testng.AssertJUnit.*;

/**
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test (testName = "client.hotrod.DefaultExpirationTest", groups = "functional" )
public class DefaultExpirationTest extends SingleCacheManagerTest {
   private Marshaller marshaller = new JBossMarshaller();

   private RemoteCache<String, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;
   private Cache<ByteArrayKey, CacheValue> cache;
   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.expiration().lifespan(3, TimeUnit.SECONDS).maxIdle(2, TimeUnit.SECONDS);
      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cache = cacheManager.getCache();

      //pass the config file to the cache
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      return new RemoteCacheManager(config);
   }


   @AfterClass(alwaysRun = true)
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   @Test
   public void testDefaultExpiration() throws Exception {
      remoteCache.put("Key", "Value");
      InternalCacheEntry entry = getInternalCacheEntry(cache, "Key", "Value");
      assertTrue(entry.canExpire());
      assertEquals(3000, entry.getLifespan());
      assertEquals(2000, entry.getMaxIdle());
      Thread.sleep(5000);
      assertFalse(remoteCache.containsKey("Key"));
   }

   private InternalCacheEntry getInternalCacheEntry(Cache<ByteArrayKey, CacheValue> cache, String key, String value) throws Exception {
      InternalCacheEntry entry = cache.getAdvancedCache().getDataContainer().get(toBinaryKey(key));
      if (value != null) {
         CacheValue v = (CacheValue) entry.getValue();
         AssertJUnit.assertEquals(toBinaryValue(value), v.data());
      }
      return entry;
   }

   private ByteArrayKey toBinaryKey(String key) throws Exception {
      byte[] keyBytes = marshaller.objectToByteBuffer(key, 64);
      return new ByteArrayKey(keyBytes);
   }

   private byte[] toBinaryValue(String value) throws Exception {
      return marshaller.objectToByteBuffer(value, 64);
   }

}
