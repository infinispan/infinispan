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
package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.JBossMarshaller;
import org.infinispan.server.core.CacheValue;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ByteArrayKey;            
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;


/**
 * @author mmarkus
 * @since 4.1
 */
@Test (testName = "client.hotrod.HotRodIntegrationTest", groups = "functional" )
public class HotRodIntegrationTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(HotRodIntegrationTest.class);

   private static final String CACHE_NAME = "replSync";
   private Cache cache;
   private Cache defaultCache;

   RemoteCache defaultRemote;
   RemoteCache remoteCache;
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Configuration standaloneConfig = getDefaultStandaloneConfig(false);
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      cacheManager.defineConfiguration(CACHE_NAME, standaloneConfig);
      defaultCache = cacheManager.getCache();
      cache = cacheManager.getCache(CACHE_NAME);


      //pass the config file to the cache
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();
      defaultRemote = remoteCacheManager.getCache();
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      return new RemoteCacheManager(config);
   }


   @AfterClass 
   public void testDestroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   public void testPut() throws Exception {
      assert null == remoteCache.put("aKey", "aValue");
      assertCacheContains(cache, "aKey", "aValue");
      assert null == defaultRemote.put("otherKey", "otherValue");
      assertCacheContains(defaultCache, "otherKey", "otherValue");
      assert remoteCache.containsKey("aKey");
      assert defaultRemote.containsKey("otherKey");
      assert remoteCache.get("aKey").equals("aValue");
      assert defaultRemote.get("otherKey").equals("otherValue");
   }

   public void testRemove() throws Exception {
      assert null == remoteCache.put("aKey", "aValue");
      assertCacheContains(cache, "aKey", "aValue");

      assert remoteCache.get("aKey").equals("aValue");

      assert null == remoteCache.remove("aKey");
      assertCacheContains(cache, "aKey", null);
      assert !remoteCache.containsKey("aKey");
   }

   public void testContains() {
      assert !remoteCache.containsKey("aKey");
      remoteCache.put("aKey", "aValue");
      assert remoteCache.containsKey("aKey");
   }

   public void testGetVersionedCacheEntry() {
      VersionedValue value = remoteCache.getVersioned("aKey");
      assertNull("expected null but received: " + value, remoteCache.getVersioned("aKey"));
      remoteCache.put("aKey", "aValue");
      assert remoteCache.get("aKey").equals("aValue");
      VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert valueBinary != null;
      assertEquals(valueBinary.getValue(), "aValue");
      log.info("Version is: " + valueBinary.getVersion());

      //now put the same value
      remoteCache.put("aKey", "aValue");
      VersionedValue entry2 = remoteCache.getVersioned("aKey");
      assertEquals(entry2.getValue(), "aValue");

      assert entry2.getVersion() != valueBinary.getVersion();
      assert !valueBinary.equals(entry2);

      //now put a different value
      remoteCache.put("aKey", "anotherValue");
      VersionedValue entry3 = remoteCache.getVersioned("aKey");
      assertEquals(entry3.getValue(), "anotherValue");
      assert entry3.getVersion() != entry2.getVersion();
      assert !entry3.equals(entry2);
   }

   public void testReplace() {
      assert null == remoteCache.replace("aKey", "anotherValue");
      remoteCache.put("aKey", "aValue");
      assert null == remoteCache.replace("aKey", "anotherValue");
      assert remoteCache.get("aKey").equals("anotherValue");
   }

   public void testReplaceIfUnmodified() {
      assert null == remoteCache.replace("aKey", "aValue");


      remoteCache.put("aKey", "aValue");
      VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert remoteCache.replaceWithVersion("aKey", "aNewValue", valueBinary.getVersion());

      VersionedValue entry2 = remoteCache.getVersioned("aKey");
      assert entry2.getVersion() != valueBinary.getVersion();
      assertEquals(entry2.getValue(), "aNewValue");

      assert !remoteCache.replaceWithVersion("aKey", "aNewValue", valueBinary.getVersion());
   }

   public void testReplaceIfUnmodifiedWithExpiry(Method m) throws InterruptedException {
      final int key = 1;
      remoteCache.put(key, v(m));
      VersionedValue valueBinary = remoteCache.getVersioned(key);
      int lifespanSecs = 3; // seconds
      long lifespan = TimeUnit.SECONDS.toMillis(lifespanSecs);
      long startTime = System.currentTimeMillis();
      String newValue = v(m, 2);
      assert remoteCache.replaceWithVersion(key, newValue, valueBinary.getVersion(), lifespanSecs);

      while (true) {
         Object value = remoteCache.get(key);
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assertEquals(v(m, 2), value);
         Thread.sleep(100);
      }

      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (remoteCache.get(key) == null) break;
         Thread.sleep(50);
      }

      assertNull(remoteCache.get(key));
   }

   public void testRemoveIfUnmodified() {
      assert !remoteCache.removeWithVersion("aKey", 12321212l);

      remoteCache.put("aKey", "aValue");
      VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert remoteCache.removeWithVersion("aKey", valueBinary.getVersion());
      assert !cache.containsKey("aKey");

      remoteCache.put("aKey", "aNewValue");

      VersionedValue entry2 = remoteCache.getVersioned("aKey");
      assert entry2.getVersion() != valueBinary.getVersion();
      assertEquals(entry2.getValue(), "aNewValue");

      assert  !remoteCache.removeWithVersion("aKey", valueBinary.getVersion());
   }

   public void testPutIfAbsent() {
      remoteCache.put("aKey", "aValue");
      assert null == remoteCache.putIfAbsent("aKey", "anotherValue");
      assertEquals(remoteCache.get("aKey"),"aValue");

      assertEquals(remoteCache.get("aKey"),"aValue");
      assert remoteCache.containsKey("aKey");

      assert true : remoteCache.replace("aKey", "anotherValue");
   }

   public void testClear() {
      remoteCache.put("aKey", "aValue");
      remoteCache.put("aKey2", "aValue");
      remoteCache.clear();
      assert !remoteCache.containsKey("aKey");
      assert !remoteCache.containsKey("aKey2");
      assert cache.isEmpty();
   }

   private void assertCacheContains(Cache cache, String key, String value) throws Exception {
      Marshaller marshaller = new JBossMarshaller();
      byte[] keyBytes = marshaller.objectToByteBuffer(key, 64);
      byte[] valueBytes = marshaller.objectToByteBuffer(value, 64);
      ByteArrayKey cacheKey = new ByteArrayKey(keyBytes);
      CacheValue cacheValue = (CacheValue) cache.get(cacheKey);
      if (value == null) {
         assert cacheValue == null : "Expected null value but received: " + cacheValue;
      } else {
         assert Arrays.equals(valueBytes, (byte[])cacheValue.data());
      }
   }
}
