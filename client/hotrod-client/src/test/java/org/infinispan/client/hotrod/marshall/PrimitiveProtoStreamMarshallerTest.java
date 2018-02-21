package org.infinispan.client.hotrod.marshall;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests integration between HotRod client and ProtoStream marshalling library with primitive types.
 *
 * @author anistor@redhat.com
 * @since 7.1
 */
@Test(testName = "client.hotrod.marshall.PrimitiveProtoStreamMarshallerTest", groups = "functional")
public class PrimitiveProtoStreamMarshallerTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testPutAndGet() {
      putAndGet(1, "bar");
      putAndGet(1, true);
      putAndGet(1, 7);
      putAndGet(1, 777L);
      putAndGet(1, 0.0);
      putAndGet(1, 1.0d);
   }

   private void putAndGet(Object key, Object value) {
      remoteCache.clear();

      remoteCache.put(key, value);
      assertTrue(remoteCache.keySet().contains(key));
      Object remoteValue = remoteCache.get(key);
      assertEquals(value, remoteValue);

      assertEquals(1, cache.keySet().size());
      Object localKey = cache.keySet().iterator().next();
      assertTrue(localKey instanceof byte[]);
      Object localObject = cache.get(localKey);
      assertNotNull(localObject);
      assertTrue(localObject instanceof byte[]);
   }
}
