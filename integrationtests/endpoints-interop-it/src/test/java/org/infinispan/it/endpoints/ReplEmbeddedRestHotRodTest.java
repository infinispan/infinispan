package org.infinispan.it.endpoints;

import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests embedded, Hot Rod and REST in a replicated clustered environment.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.endpoints.ReplEmbeddedRestHotRodTest")
public class ReplEmbeddedRestHotRodTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<Object, Object> cacheFactory1;
   EndpointsCacheFactory<Object, Object> cacheFactory2;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory1 = new EndpointsCacheFactory.Builder<>().withCacheMode(CacheMode.REPL_SYNC).build();
      cacheFactory2 = new EndpointsCacheFactory.Builder<>().withCacheMode(CacheMode.REPL_SYNC).build();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

   public void testRestPutEmbeddedHotRodGet() {
      final String key = "1";

      // 1. Put with REST
      RestEntity value = RestEntity.create(MediaType.TEXT_PLAIN, "<hey>ho</hey>".getBytes());
      RestResponse response = join(cacheFactory1.getRestCacheClient().put(key, value));
      assertEquals(204, response.getStatus());

      // 2. Get with Embedded
      Cache embeddedCache = cacheFactory2.getEmbeddedCache().getAdvancedCache();
      assertEquals("<hey>ho</hey>", embeddedCache.get(key));

      // 3. Get with Hot Rod
      assertEquals("<hey>ho</hey>", cacheFactory2.getHotRodCache().get(key));
   }

   public void testEmbeddedPutRestHotRodGet() {
      final String key = "2";

      // 1. Put with Embedded
      Cache cache = cacheFactory2.getEmbeddedCache().getAdvancedCache();
      assertNull(cache.put(key, "v1"));

      // 2. Get with Hot Rod via remote client, will use the configured encoding
      assertEquals("v1", cacheFactory1.getHotRodCache().get(key));

      // 3. Get with REST, specifying the results as 'text'
      RestResponse response = join(cacheFactory2.getRestCacheClient().get(key, TEXT_PLAIN_TYPE));

      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
   }

   public void testHotRodPutEmbeddedRestGet() {
      final String key = "3";

      // 1. Put with Hot Rod
      RemoteCache<Object, Object> remote = cacheFactory1.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      Cache embeddedCache = cacheFactory2.getEmbeddedCache().getAdvancedCache();
      assertEquals("v1", embeddedCache.get(key));

      // 3. Get with REST
      RestResponse response = join(cacheFactory2.getRestCacheClient().get(key, TEXT_PLAIN_TYPE));

      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
   }
}
