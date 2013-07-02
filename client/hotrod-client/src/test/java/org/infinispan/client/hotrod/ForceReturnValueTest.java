package org.infinispan.client.hotrod;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertNull;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;

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
      // This method should be limited to starting the cache manager, to avoid
      // leaks as a result of code after creating the cache manager failing.
      return TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost",hotrodServer.getPort());
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass
   public void destroy() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
      super.teardown();
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

   public void testRemoveNonExistForceReturnPrevious() {
      assertNull(remoteCache.withFlags(Flag.FORCE_RETURN_VALUE).remove("aKey"));
      remoteCache.put("k", "v");
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
