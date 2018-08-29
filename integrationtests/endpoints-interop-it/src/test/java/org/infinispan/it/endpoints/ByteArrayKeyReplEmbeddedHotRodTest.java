package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded and Hot Rod in a replicated clustered environment using byte array keys.
 *
 * @author Martin Gencur
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "it.interop.ByteArrayKeyReplEmbeddedHotRodTest")
public class ByteArrayKeyReplEmbeddedHotRodTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<Object, Object> cacheFactory1;
   EndpointsCacheFactory<Object, Object> cacheFactory2;

   public void testHotRodPutEmbeddedGet() throws Exception {
      final byte[] key = "4".getBytes();
      final String value = "v1";

      // 1. Put with HotRod
      RemoteCache<Object, Object> remote = cacheFactory1.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, value));

      // 2. Get with Embedded
      assertEquals(value, cacheFactory2.getEmbeddedCache().get(key));
   }

   public void testHotRodReplace() throws Exception {
      final byte[] key = "5".getBytes();
      final String value1 = "v1";
      final String value2 = "v2";

      // 1. Put with HotRod
      RemoteCache<Object, Object> remote = cacheFactory1.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, value1));

      // 2. Replace with HotRod
      VersionedValue versioned = cacheFactory1.getHotRodCache().getVersioned(key);
      assertTrue(cacheFactory1.getHotRodCache().replaceWithVersion(key, value2, versioned.getVersion()));
   }

   public void testHotRodRemove() throws Exception {
      final byte[] key = "7".getBytes();
      final String value = "v1";

      // 1. Put with HotRod
      RemoteCache<Object, Object> remote = cacheFactory1.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, value));

      // 2. Removed with HotRod
      VersionedValue versioned = cacheFactory1.getHotRodCache().getVersioned(key);
      assertTrue(cacheFactory1.getHotRodCache().removeWithVersion(key, versioned.getVersion()));
   }

   //This test can fail only if there's a marshaller specified for EmbeddedTypeConverter
   public void testEmbeddedPutHotRodGet() throws Exception {
      final byte[] key = "8".getBytes();
      final String value = "v1";

      // 1. Put with Embedded
      assertNull(cacheFactory2.getEmbeddedCache().put(key, value));

      // 2. Get with HotRod
      assertEquals(value, cacheFactory1.getHotRodCache().get(key));
   }

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new EndpointsCacheFactory<>(CacheMode.REPL_SYNC).setup();
      cacheFactory2 = new EndpointsCacheFactory<>(CacheMode.REPL_SYNC).setup();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }
}
