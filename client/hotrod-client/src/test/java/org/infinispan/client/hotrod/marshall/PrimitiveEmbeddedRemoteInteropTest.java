package org.infinispan.client.hotrod.marshall;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests interoperability between remote and embedded with primitive types
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.marshall.PrimitiveEmbeddedRemoteInteropTest", groups = "functional")
public class PrimitiveEmbeddedRemoteInteropTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;
   private Cache embeddedCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = createConfigBuilder();

      cacheManager = TestCacheManagerFactory.createServerModeCacheManager(builder);
      cache = cacheManager.getCache();

      embeddedCache = cache.getAdvancedCache().withEncoding(IdentityEncoder.class);

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   protected org.infinispan.configuration.cache.ConfigurationBuilder createConfigBuilder() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      return builder;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testRemotePutAndGet() {
      remotePutAndGet(1, "foo");
      remotePutAndGet(1, true);
      remotePutAndGet(1, 7);
      remotePutAndGet(1, 777L);
      remotePutAndGet(1, 0.0);
      remotePutAndGet(1, 1.0d);
   }

   private void remotePutAndGet(Object key, Object value) {
      remoteCache.clear();

      remoteCache.put(key, value);
      Object remoteValue = remoteCache.get(key);
      assertEquals(value, remoteValue);

      // try to get the value through the embedded cache interface and check it's equals with the value we put
      assertEquals(1, embeddedCache.keySet().size());
      Object localKey = embeddedCache.keySet().iterator().next();
      assertEquals(key, localKey);
      Object localObject = embeddedCache.get(localKey);
      assertEquals(value, localObject);
   }

   public void testEmbeddedPutAndGet() {
      embeddedPutAndGet(1, "bar");
      embeddedPutAndGet(1, true);
      embeddedPutAndGet(1, 7);
      embeddedPutAndGet(1, 777L);
      embeddedPutAndGet(1, 0.0);
      embeddedPutAndGet(1, 1.0d);
   }

   private void embeddedPutAndGet(Object key, Object value) {
      embeddedCache.clear();

      embeddedCache.put(key, value);
      assertTrue(embeddedCache.keySet().contains(key));
      Object localValue = embeddedCache.get(key);
      assertEquals(value, localValue);

      // try to get the value through the remote cache interface and check it's equals with the value we put
      assertEquals(1, remoteCache.keySet().size());
      Object remoteKey = remoteCache.keySet().iterator().next();
      assertEquals(key, remoteKey);
      Object remoteValue = remoteCache.get(remoteKey);
      assertEquals(value, remoteValue);
   }
}
