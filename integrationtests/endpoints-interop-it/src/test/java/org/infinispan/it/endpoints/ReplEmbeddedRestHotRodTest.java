package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.IdentityEncoder;
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
      cacheFactory1 = new EndpointsCacheFactory<>(CacheMode.REPL_SYNC).setup();
      cacheFactory2 = new EndpointsCacheFactory<>(CacheMode.REPL_SYNC).setup();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory1, cacheFactory2);
   }

   public void testRestPutEmbeddedHotRodGet() throws Exception {
      final String key = "1";

      // 1. Put with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory1.getRestUrl() + "/" + key);
      put.setRequestEntity(new ByteArrayRequestEntity(
            "<hey>ho</hey>".getBytes(), MediaType.TEXT_PLAIN_TYPE));
      HttpClient restClient = cacheFactory1.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpStatus.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 2. Get with Embedded
      Cache embeddedCache = cacheFactory2.getEmbeddedCache().getAdvancedCache();
      assertEquals("<hey>ho</hey>", embeddedCache.get(key));

      // 3. Get with Hot Rod
      assertEquals("<hey>ho</hey>", cacheFactory2.getHotRodCache().get(key));
   }

   public void testEmbeddedPutRestHotRodGet() throws Exception {
      final String key = "2";

      // 1. Put with Embedded, bypassing all encodings
      Cache cache = cacheFactory2.getEmbeddedCache().getAdvancedCache().withEncoding(IdentityEncoder.class);
      assertEquals(null, cache.put(key, "v1"));

      // 2. Get with Hot Rod via remote client, will use the configured encoding
      assertEquals("v1", cacheFactory1.getHotRodCache().get(key));

      // 3. Get with REST, specifying the results as 'text'
      HttpMethod get = new GetMethod(cacheFactory2.getRestUrl() + "/" + key);
      get.setRequestHeader("Accept", "text/plain");

      cacheFactory2.getRestClient().executeMethod(get);
      assertEquals(HttpStatus.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }

   public void testHotRodPutEmbeddedRestGet() throws Exception {
      final String key = "3";

      // 1. Put with Hot Rod
      RemoteCache<Object, Object> remote = cacheFactory1.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));

      // 2. Get with Embedded
      Cache embeddedCache = cacheFactory2.getEmbeddedCache().getAdvancedCache().withEncoding(IdentityEncoder.class);
      assertEquals("v1", embeddedCache.get(key));

      // 3. Get with REST
      HttpMethod get = new GetMethod(cacheFactory2.getRestUrl() + "/" + key);
      get.setRequestHeader("Accept", "text/plain");
      cacheFactory2.getRestClient().executeMethod(get);
      assertEquals(HttpStatus.SC_OK, get.getStatusCode());
      assertEquals("v1", get.getResponseBodyAsString());
   }
}
