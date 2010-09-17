package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ForceReturnValueTest", groups = "functional")
public class ForceReturnValueTest extends SingleCacheManagerTest {

   RemoteCache remoteCache;
   
   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cache = cacheManager.getCache();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);

      remoteCacheManager = new RemoteCacheManager("localhost",hotrodServer.getPort());
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }


   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      remoteCacheManager.stop();
      hotrodServer.stop();
   }

   public void testPut() {
      assert null == remoteCache.put("aKey", "aValue");
      assert "aValue".equals(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).put("aKey", "otherValue"));
      assert remoteCache.containsKey("aKey");
      assert remoteCache.get("aKey").equals("otherValue");
   }

   public void testRemove() {
      assert null == remoteCache.put("aKey", "aValue");
      assert remoteCache.get("aKey").equals("aValue");
      assert "aValue".equals(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove("aKey"));
      assert !remoteCache.containsKey("aKey");
   }

   public void testContains() {
      assert !remoteCache.containsKey("aKey");
      remoteCache.put("aKey", "aValue");
      assert remoteCache.containsKey("aKey");
   }

   public void testReplace() {
      assert null == remoteCache.replace("aKey", "anotherValue");
      remoteCache.put("aKey", "aValue");
      assert "aValue".equals(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).replace("aKey", "anotherValue"));
      assert remoteCache.get("aKey").equals("anotherValue");
   }

   public void testPutIfAbsent() {
      remoteCache.put("aKey", "aValue");
      assert null == remoteCache.putIfAbsent("aKey", "anotherValue");
      Object existingValue = remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent("aKey", "anotherValue");
      assert "aValue".equals(existingValue) : "Existing value was:" + existingValue;
   }
}
