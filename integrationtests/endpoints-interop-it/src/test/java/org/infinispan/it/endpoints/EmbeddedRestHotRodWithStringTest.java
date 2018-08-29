package org.infinispan.it.endpoints;

import static org.testng.AssertJUnit.assertEquals;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.marshall.UTF8StringMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "it.endpoints.EmbeddedRestHotRodWithStringTest")
public class EmbeddedRestHotRodWithStringTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<String, Object> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory<String, Object>("testCache", new UTF8StringMarshaller(), CacheMode.LOCAL).setup();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testRestPutStringHotRodGet() throws Exception {
      final String key = "1";

      // 1. Put text content with REST
      EntityEnclosingMethod put = new PutMethod(cacheFactory.getRestUrl() + "/" + key);
      put.setRequestEntity(new StringRequestEntity("<hey>ho</hey>", "text/plain", "UTF-8"));
      HttpClient restClient = cacheFactory.getRestClient();
      restClient.executeMethod(put);
      assertEquals(HttpStatus.SC_OK, put.getStatusCode());
      assertEquals("", put.getResponseBodyAsString().trim());

      // 3. Get with Hot Rod
      assertEquals("<hey>ho</hey>", cacheFactory.getHotRodCache().get(key));

      final String newKey = "2";
      final String newValue = "<let's>go</let's>";

      //4. Put text content with Hot Rod
      RemoteCache<String, Object> hotRodCache = cacheFactory.getHotRodCache();
      hotRodCache.put(newKey, newValue);

      //5. Read with rest
      HttpMethod get = new GetMethod(cacheFactory.getRestUrl() + "/" + newKey);
      cacheFactory.getRestClient().executeMethod(get);
      assertEquals(HttpStatus.SC_OK, get.getStatusCode());
      assertEquals(newValue, get.getResponseBodyAsString());
   }

}
