package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author mmarkus
 * @since 4.1
 */
@Test (testName = "client.hotrod.HotRodIntegrationTest", groups = {"functional", "smoke"} )
public class HotRodIntegrationTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(HotRodIntegrationTest.class);

   private static final String CACHE_NAME = "replSync";

   RemoteCache<String, String> defaultRemote;
   RemoteCache<Object, String> remoteCache;
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      EmbeddedCacheManager cm = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration());
      cm.defineConfiguration(CACHE_NAME, builder.build());
      cm.getCache(CACHE_NAME);
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      //pass the config file to the cache
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      remoteCacheManager = getRemoteCacheManager();
      defaultRemote = remoteCacheManager.getCache();
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      return new RemoteCacheManager(clientBuilder.build());
   }


   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
   }

   public void testPut() throws Exception {
      assert null == remoteCache.put("aKey", "aValue");
      assertHotRodEquals(cacheManager, CACHE_NAME, "aKey", "aValue");
      assert null == defaultRemote.put("otherKey", "otherValue");
      assertHotRodEquals(cacheManager, "otherKey", "otherValue");
      assert remoteCache.containsKey("aKey");
      assert defaultRemote.containsKey("otherKey");
      assert remoteCache.get("aKey").equals("aValue");
      assert defaultRemote.get("otherKey").equals("otherValue");
   }

   public void testRemove() throws Exception {
      assert null == remoteCache.put("aKey", "aValue");
      assertHotRodEquals(cacheManager, CACHE_NAME, "aKey", "aValue");

      assert remoteCache.get("aKey").equals("aValue");

      assert null == remoteCache.remove("aKey");
      assertHotRodEquals(cacheManager, CACHE_NAME, "aKey", null);
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

   public void testGetWithMetadata() {
      MetadataValue<?> value = remoteCache.getWithMetadata("aKey");
      assertNull("expected null but received: " + value, value);
      remoteCache.put("aKey", "aValue");
      assert remoteCache.get("aKey").equals("aValue");
      MetadataValue<?> immortalValue = remoteCache.getWithMetadata("aKey");
      assertNotNull(immortalValue);
      assertEquals("aValue", immortalValue.getValue());
      assertEquals(-1, immortalValue.getLifespan());
      assertEquals(-1, immortalValue.getMaxIdle());

      remoteCache.put("bKey", "bValue", 60, TimeUnit.SECONDS);
      MetadataValue<?> mortalValueWithLifespan = remoteCache.getWithMetadata("bKey");
      assertNotNull(mortalValueWithLifespan);
      assertEquals("bValue", mortalValueWithLifespan.getValue());
      assertEquals(60, mortalValueWithLifespan.getLifespan());
      assertEquals(-1, mortalValueWithLifespan.getMaxIdle());

      remoteCache.put("cKey", "cValue", 60, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
      MetadataValue<?> mortalValueWithMaxIdle = remoteCache.getWithMetadata("cKey");
      assertNotNull(mortalValueWithMaxIdle);
      assertEquals("cValue", mortalValueWithMaxIdle.getValue());
      assertEquals(60, mortalValueWithMaxIdle.getLifespan());
      assertEquals(30, mortalValueWithMaxIdle.getMaxIdle());
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

   public void testReplaceWithVersionWithLifespanAsync(Method m) throws Exception {
      int lifespanInSecs = 1; //seconds
      final String k = k(m), v = v(m), newV = v(m, 2);
      assertNull(remoteCache.replace(k, v));

      remoteCache.put(k, v);
      VersionedValue valueBinary = remoteCache.getVersioned(k);
      long lifespan = TimeUnit.SECONDS.toMillis(lifespanInSecs);
      long startTime = System.currentTimeMillis();
      CompletableFuture<Boolean> future = remoteCache.replaceWithVersionAsync(
         k, newV, valueBinary.getVersion(), lifespanInSecs);
      assert future.get();

      while (true) {
         VersionedValue entry2 = remoteCache.getVersioned(k);
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         // version should have changed; value should have changed
         assert entry2.getVersion() != valueBinary.getVersion();
         assertEquals(newV, entry2.getValue());
         Thread.sleep(100);
      }

      while (System.currentTimeMillis() < startTime + lifespan + 2000) {
         if (remoteCache.get(k) == null) break;
         Thread.sleep(50);
      }

      assertNull(remoteCache.getVersioned(k));
   }

   public void testRemoveIfUnmodified() {
      assert !remoteCache.removeWithVersion("aKey", 12321212l);

      remoteCache.put("aKey", "aValue");
      VersionedValue valueBinary = remoteCache.getVersioned("aKey");
      assert remoteCache.removeWithVersion("aKey", valueBinary.getVersion());
      assertHotRodEquals(cacheManager, CACHE_NAME, "aKey", null);

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

   public void testPutWithPrevious() {
      assert null == remoteCache.put("aKey", "aValue");
      assert "aValue".equals(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).put("aKey", "otherValue"));
      assert remoteCache.containsKey("aKey");
      assert remoteCache.get("aKey").equals("otherValue");
   }

   public void testRemoveWithPrevious() {
      assert null == remoteCache.put("aKey", "aValue");
      assert remoteCache.get("aKey").equals("aValue");
      assert "aValue".equals(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove("aKey"));
      assert !remoteCache.containsKey("aKey");
   }

   public void testRemoveNonExistForceReturnPrevious() {
      assertNull(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove("aKey"));
      remoteCache.put("k", "v");
   }

   public void testReplaceWithPrevious() {
      assert null == remoteCache.replace("aKey", "anotherValue");
      remoteCache.put("aKey", "aValue");
      assert "aValue".equals(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).replace("aKey", "anotherValue"));
      assert remoteCache.get("aKey").equals("anotherValue");
   }

   public void testPutIfAbsentWithPrevious() {
      remoteCache.put("aKey", "aValue");
      assert null == remoteCache.putIfAbsent("aKey", "anotherValue");
      Object existingValue = remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent("aKey", "anotherValue");
      assert "aValue".equals(existingValue) : "Existing value was:" + existingValue;
   }

   public void testPutSerializableByteArray() {
      RemoteCache<Object, byte[]> binaryRemote = remoteCacheManager.getCache();
      byte[] bytes = serializeObject(new MyValue<>("aValue"));
      binaryRemote.put("aKey", bytes);
      assertArrayEquals(bytes, binaryRemote.get("aKey"));
   }

   private byte[] serializeObject(Object obj) {
      ByteArrayOutputStream bs = new ByteArrayOutputStream();
      try {
         ObjectOutputStream os = new ObjectOutputStream(bs);
         os.writeObject(obj);
         os.close();
         return bs.toByteArray();
      } catch (IOException ioe) {
         throw new AssertionError(ioe);
      }
   }

   static class MyValue<V> implements Serializable {
      final V value;

      MyValue(V value) {
         this.value = value;
      }
   }

}
