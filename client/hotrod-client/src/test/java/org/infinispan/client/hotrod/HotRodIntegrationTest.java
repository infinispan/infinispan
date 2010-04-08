package org.infinispan.client.hotrod;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.impl.SerializationMarshaller;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.core.CacheValue;
import org.infinispan.server.hotrod.CacheKey;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;


/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
@Test (testName = "client.hotrod.HotRodClientIntegrationTest", groups = "functional", enabled = false, description = "TODO To be re-enabled when we have a multithreaded HotRod server impl") 
public class HotRodIntegrationTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "replSync";
   private Cache cache;
   private Cache defaultCache;

   RemoteCache defaultRemote;
   RemoteCache remoteCache;
   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      Configuration standaloneConfig = getDefaultStandaloneConfig(false);
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cacheManager.defineConfiguration(CACHE_NAME, standaloneConfig);
      defaultCache = cacheManager.getCache();
      cache = cacheManager.getCache(CACHE_NAME);


      //pass the config file to the cache
      hotrodServer = HotRodTestingUtil.startHotRodServer(cacheManager);

      remoteCacheManager = getRemoteCacheManager();
      defaultRemote = remoteCacheManager.getCache();
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
      return cacheManager;
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      return new RemoteCacheManager();
   }


   @AfterClass 
   public void testDestroyRemoteCacheFactory() {
      assert remoteCache.ping();
      hotrodServer.stop();
      assert !remoteCache.ping();
//      try {
//         remoteCache.get("aKey");
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.clear();
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.put("aKey", "aValue");
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.putIfAbsent("aKey", "aValue");
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.remove("aKey", 0);
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.remove("aKey");
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.replace("aKey", "aNewValue");
//         assert false;
//      } catch (ClientDisconnectedException e) {}
//      try {
//         remoteCache.replace("aKey", "aNewValue");
//         assert false;
//      } catch (ClientDisconnectedException e) {}
   }

   public void testPut() {
      assert null == remoteCache.put("aKey", "aValue");
      assertCacheContains(cache, "aKey", "aValue");
      assert null == defaultRemote.put("otherKey", "otherValue");
      assertCacheContains(defaultCache, "otherKey", "otherValue");
      assert remoteCache.containsKey("aKey");
      assert defaultRemote.containsKey("otherKey");
      assert remoteCache.get("aKey").equals("aValue");
      assert defaultRemote.get("otherKey").equals("otherValue");
   }

   public void testRemove() {
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

   private static Log log = LogFactory.getLog(HotRodIntegrationTest.class);

   public void testGetVersionedCacheEntry() {
      assert null == remoteCache.getVersioned("aKey");
      remoteCache.put("aKey", "aValue");
      assert remoteCache.get("aKey").equals("aValue");
      RemoteCache.VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert valueBinary != null;
      assertEquals(valueBinary.getValue(), "aValue");
      log.info("Version is: " + valueBinary.getVersion());

      //now put the same value
      remoteCache.put("aKey", "aValue");
      RemoteCache.VersionedValue entry2 = remoteCache.getVersioned("aKey");
      assertEquals(entry2.getValue(), "aValue");

      assert entry2.getVersion() != valueBinary.getVersion();
      assert !valueBinary.equals(entry2);

      //now put a different value
      remoteCache.put("aKey", "anotherValue");
      RemoteCache.VersionedValue entry3 = remoteCache.getVersioned("aKey");
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
      RemoteCache.VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert remoteCache.replace("aKey", "aNewValue", valueBinary.getVersion());

      RemoteCache.VersionedValue entry2 = remoteCache.getVersioned("aKey");
      assert entry2.getVersion() != valueBinary.getVersion();
      assertEquals(entry2.getValue(), "aNewValue");

      assert !remoteCache.replace("aKey", "aNewValue", valueBinary.getVersion());
   }

   public void testRemoveIfUnmodified() {
      assert !remoteCache.remove("aKey", 12321212l);

      remoteCache.put("aKey", "aValue");
      RemoteCache.VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert remoteCache.remove("aKey", valueBinary.getVersion());
      assert !cache.containsKey("aKey");

      remoteCache.put("aKey", "aNewValue");

      RemoteCache.VersionedValue entry2 = remoteCache.getVersioned("aKey");
      assert entry2.getVersion() != valueBinary.getVersion();
      assertEquals(entry2.getValue(), "aNewValue");

      assert  !remoteCache.remove("aKey", valueBinary.getVersion());
   }

   public void testPutIfAbsent() {
      remoteCache.put("aKey", "aValue");
      assert null == remoteCache.putIfAbsent("aKey", "anotherValue");
      assert remoteCache.get("aKey").equals("aValue");

      assert remoteCache.get("aKey").equals("aValue");
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

   public void testStats() {
      //todo implement
   }

   private void assertCacheContains(Cache cache, String key, String value) {
      SerializationMarshaller marshaller = new SerializationMarshaller();
      byte[] keyBytes = marshaller.marshallObject(key);
      byte[] valueBytes = marshaller.marshallObject(value);
      CacheKey cacheKey = new CacheKey(keyBytes);
      CacheValue cacheValue = (CacheValue) cache.get(cacheKey);
      if (value == null) {
         assert cacheValue == null : "Expected null value but received: " + cacheValue;
      } else {
         assert Arrays.equals(valueBytes, cacheValue.data());
      }
   }

   private Object get(Cache cache, String s) {

      return new String((byte[])cache.get(s));
   }
}
