package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests compatibility mode with primitive types.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.marshall.PrimitiveEmbeddedCompatTest", groups = "functional")
public class PrimitiveEmbeddedCompatTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = createConfigBuilder();
      // the default key equivalence works only for byte[] so we need to override it with one that works for Object
      builder.dataContainer().keyEquivalence(AnyEquivalence.getInstance());

      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   protected org.infinispan.configuration.cache.ConfigurationBuilder createConfigBuilder() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.compatibility().enable().marshaller(new CompatibilityProtoStreamMarshaller());
      return builder;
   }

   @AfterTest
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
      assertEquals(1, cache.keySet().size());
      Object localKey = cache.keySet().iterator().next();
      assertEquals(key, localKey);
      Object localObject = cache.get(localKey);
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
      cache.clear();

      cache.put(key, value);
      assertTrue(cache.keySet().contains(key));
      Object localValue = cache.get(key);
      assertEquals(value, localValue);

      // try to get the value through the remote cache interface and check it's equals with the value we put
      assertEquals(1, remoteCache.keySet().size());
      Object remoteKey = remoteCache.keySet().iterator().next();
      assertEquals(key, remoteKey);
      Object remoteValue = remoteCache.get(remoteKey);
      assertEquals(value, remoteValue);
   }
}
