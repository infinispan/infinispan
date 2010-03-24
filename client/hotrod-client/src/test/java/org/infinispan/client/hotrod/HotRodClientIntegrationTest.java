package org.infinispan.client.hotrod;

import hotrod.ClientDisconnectedException;
import hotrod.ClusterTopologyListener;
import hotrod.impl.VersionedEntry;
import hotrod.impl.RemoteCacheSpi;
import hotrod.RemoteCacheManager;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;


/**
 * // TODO: Document this
 *
 * @author mmarkus
 * @since 4.1
 */
@Test (testName = "client.hotrod.HotRodClientIntegrationTest", groups = "functional", enabled = false) 
public class HotRodClientIntegrationTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "replSync";
   private Cache cache;
   private Cache defaultCache;

   RemoteCacheSpi defaultRemoteCacheSpi;
   RemoteCacheSpi remoteCacheSpi;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      createClusteredCaches(2, CACHE_NAME, replSync);

      //pass the config file to the cache
      RemoteCacheManager cacheManager = new RemoteCacheManager();
      defaultRemoteCacheSpi = cacheManager.getDefaultRemoteCache();
      remoteCacheSpi = cacheManager.getRemoteCache(CACHE_NAME);
   }

   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      RemoteCacheManager cacheManager = remoteCacheSpi.getRemoteCacheFactory();
      cacheManager.stop();
      try {
         remoteCacheSpi.get("aKey".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.clear();
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.evict("aKey".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.addClusterTopologyListener(null);
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.removeClusterTopologyListener(null);
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.putForExternalRead("aKey".getBytes(), "aValue".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.putIfAbsent("aKey".getBytes(), "aValue".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.remove("aKey".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.removeIfUnmodified("aKey".getBytes(), 12321L);
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.replaceIfUnmodified("aKey".getBytes(), "aNewValue".getBytes(), 12321L);
         assert false;
      } catch (ClientDisconnectedException e) {}
      try {
         remoteCacheSpi.replace("aKey".getBytes(), "aNewValue".getBytes());
         assert false;
      } catch (ClientDisconnectedException e) {}
   }

   @AfterClass (alwaysRun = true)
   @Override
   protected void destroy() {
      TestTopologyListener listener = new TestTopologyListener();
      remoteCacheSpi.addClusterTopologyListener(listener);
      super.destroy();
      assert listener.invocationCount == 2; 
   }

   public void testPut() {
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      assertEquals("aValue", get(cache, "aKey"));
      defaultRemoteCacheSpi.put("otherKey".getBytes(), "otherValue".getBytes());
      assertEquals("otherValue", get(defaultCache, "otherKey"));

      assert Arrays.equals("aKey".getBytes(), remoteCacheSpi.get("aValue".getBytes()));
      assert Arrays.equals("otherKey".getBytes(), remoteCacheSpi.get("otherKey".getBytes()));
   }

   public void testRemove() {
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      assert Arrays.equals("aKey".getBytes(), remoteCacheSpi.get("aValue".getBytes()));

      assert get(cache, "aKey").equals("aValue");
      assertEquals(true, remoteCacheSpi.remove("aKey".getBytes()));
      assert remoteCacheSpi.remove("aKey".getBytes());
      assert get(cache,"aKey") == null;
      assert !remoteCacheSpi.remove("aKey".getBytes());
   }

   public void testContains() {
      assert !remoteCacheSpi.contains("aKey".getBytes());
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      assert !remoteCacheSpi.contains("aKey".getBytes());
   }

   public void testGetVersionedCacheEntry() {
      assert null == remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      VersionedEntry entry = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      assert entry != null;
      assert Arrays.equals(entry.getKey(), "aKey".getBytes());
      assert Arrays.equals(entry.getValue(), "aValue".getBytes());

      //now put the same value
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      VersionedEntry entry2 = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      assert Arrays.equals(entry2.getKey(), "aKey".getBytes());
      assert Arrays.equals(entry2.getValue(), "aValue".getBytes());

      assert entry2.getVersion() != entry.getVersion();
      assert !entry.equals(entry2);

      //now put a different value
      remoteCacheSpi.put("aKey".getBytes(), "anotherValue".getBytes());
      VersionedEntry entry3 = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      assert Arrays.equals(entry3.getKey(), "aKey".getBytes());
      assert Arrays.equals(entry3.getValue(), "anotherValue".getBytes());
      assert entry3.getVersion() != entry2.getVersion();
      assert !entry3.equals(entry2);
   }

   public void testReplace() {
      assert !remoteCacheSpi.replace("aKey".getBytes(), "anotherValue".getBytes());
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      assert remoteCacheSpi.replace("aKey".getBytes(), "anotherValue".getBytes());
      assert get(cache, "aKey").equals("anotherValue");
   }

   public void testReplaceIfUnmodified() {
      RemoteCacheSpi.VersionedOperationResponse response = remoteCacheSpi.replaceIfUnmodified("aKey".getBytes(), "aValue".getBytes(), 12321212l);
      assert response == RemoteCacheSpi.VersionedOperationResponse.NO_SUCH_KEY;
      assert !response.isUpdated();

      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      VersionedEntry entry = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      response = remoteCacheSpi.replaceIfUnmodified("aKey".getBytes(), "aNewValue".getBytes(), entry.getVersion());
      assert response == RemoteCacheSpi.VersionedOperationResponse.SUCCESS;
      assert response.isUpdated();

      VersionedEntry entry2 = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      assert entry2.getVersion() != entry.getVersion();
      assert Arrays.equals(entry2.getKey(), "aKey".getBytes());
      assert Arrays.equals(entry2.getValue(), "aNewValue".getBytes());

      response = remoteCacheSpi.replaceIfUnmodified("aKey".getBytes(), "aNewValue".getBytes(), entry.getVersion());
      assert response == RemoteCacheSpi.VersionedOperationResponse.MODIFIED_KEY;
      assert !response.isUpdated();
   }

   public void testRemoveIfUnmodified() {
      RemoteCacheSpi.VersionedOperationResponse response = remoteCacheSpi.removeIfUnmodified("aKey".getBytes(), 12321212l);
      assert response == RemoteCacheSpi.VersionedOperationResponse.NO_SUCH_KEY;
      assert !response.isUpdated();

      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      VersionedEntry entry = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      response = remoteCacheSpi.removeIfUnmodified("aKey".getBytes(), entry.getVersion());
      assert response == RemoteCacheSpi.VersionedOperationResponse.SUCCESS;
      assert response.isUpdated();
      assert !cache.containsKey("aKey".getBytes());

      remoteCacheSpi.put("aKey".getBytes(), "aValueNew".getBytes());

      VersionedEntry entry2 = remoteCacheSpi.getVersionedCacheEntry("aKey".getBytes());
      assert entry2.getVersion() != entry.getVersion();
      assert Arrays.equals(entry2.getKey(), "aKey".getBytes());
      assert Arrays.equals(entry2.getValue(), "aNewValue".getBytes());

      response = remoteCacheSpi.removeIfUnmodified("aKey".getBytes(), entry.getVersion());
      assert response == RemoteCacheSpi.VersionedOperationResponse.MODIFIED_KEY;
      assert !response.isUpdated();
   }

   public void testPutIfAbsent() {
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      assert !remoteCacheSpi.putIfAbsent("aKey".getBytes(), "anotherValue".getBytes());
      assert get(cache, "aKey").equals("aValue");

      assert cache.remove("aKey".getBytes()).equals("aValue".getBytes());
      assert !remoteCacheSpi.contains("aKey".getBytes());

      assert true : remoteCacheSpi.replace("aKey".getBytes(), "anotherValue".getBytes());
   }

   public void testPutForExternalRead() {
      remoteCacheSpi.putForExternalRead("aKey".getBytes(), "aValue".getBytes());
      remoteCacheSpi.putForExternalRead("aKey".getBytes(), "anotherValue".getBytes());
      assert get(cache, "aKey").equals("aValue");

      assert cache.remove("aKey".getBytes()).equals("aValue".getBytes());
      assert !remoteCacheSpi.contains("aKey".getBytes());
   }

   public void testClear() {
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      remoteCacheSpi.put("aKey2".getBytes(), "aValue".getBytes());
      remoteCacheSpi.clear();
      assert cache.isEmpty();
      assert !remoteCacheSpi.contains("aKey".getBytes());
      assert !remoteCacheSpi.contains("aKey2".getBytes());
   }

   public void testEvict() {
      assert  !remoteCacheSpi.evict("aKey".getBytes());
      remoteCacheSpi.put("aKey".getBytes(), "aValue".getBytes());
      assert remoteCacheSpi.evict("aKey".getBytes());
   }

   public void testStats() {
      //todo implement
   }

   private Object get(Cache cache, String s) {
      return new String((byte[])cache.get(s.getBytes()));
   }

   private static class TestTopologyListener implements ClusterTopologyListener {

      private int invocationCount;

      public void nodeAdded(List<Address> currentTopology, Address addedNode) {
         // TODO: Customise this generated block
      }

      public void nodeRemoved(List<Address> currentTopology, Address removedNode) {
         invocationCount++;
      }
   }
}
