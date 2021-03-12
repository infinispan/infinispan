package org.infinispan.it.endpoints;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
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
      cacheFactory = new EndpointsCacheFactory.Builder<String, Object>().withCacheName("testCache")
            .withMarshaller(new UTF8StringMarshaller()).withCacheMode(CacheMode.LOCAL).build();
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testRestPutStringHotRodGet() {
      final String key = "1";

      // 1. Put text content with REST
      RestEntity value = RestEntity.create(MediaType.TEXT_PLAIN, "<hey>ho</hey>");
      RestResponse response = join(cacheFactory.getRestCacheClient().put(key, value));
      assertEquals(204, response.getStatus());

      // 3. Get with Hot Rod
      assertEquals("<hey>ho</hey>", cacheFactory.getHotRodCache().get(key));

      final String newKey = "2";
      final String newValue = "<let's>go</let's>";

      //4. Put text content with Hot Rod
      RemoteCache<String, Object> hotRodCache = cacheFactory.getHotRodCache();
      hotRodCache.put(newKey, newValue);

      //5. Read with rest
      response = join(cacheFactory.getRestCacheClient().get(newKey));
      assertEquals(200, response.getStatus());
      assertEquals(newValue, response.getBody());
   }

}
