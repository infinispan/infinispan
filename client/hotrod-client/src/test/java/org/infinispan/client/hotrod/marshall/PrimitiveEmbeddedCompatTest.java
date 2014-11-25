package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;

/**
 * Tests compatibility mode with primitive types.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.marshall.PrimitiveEmbeddedCompatTest", groups = "functional")
@CleanupAfterMethod
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

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

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

   public void testRemotePutAndGet() throws Exception {
      remoteCache.put(1, "foo");
      Object remoteObject = remoteCache.get(1);
      assertEquals("foo", remoteObject);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, cache.keySet().size());
      Object key = cache.keySet().iterator().next();
      Object localObject = cache.get(key);
      assertEquals("foo", localObject);
   }

   public void testLocalPutAndGet() throws Exception {
      cache.put(1, "bar");
      Object localObject = cache.get(1);
      assertEquals("bar", localObject);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, remoteCache.keySet().size());
      Object key = remoteCache.keySet().iterator().next();
      Object remoteObject = remoteCache.get(key);
      assertEquals("bar", remoteObject);
   }
}
