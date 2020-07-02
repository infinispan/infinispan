package org.infinispan.marshaller.test;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.it.endpoints.EmbeddedRestMemcachedHotRodTest;
import org.infinispan.it.endpoints.EndpointsCacheFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "functional")
public abstract class AbstractInteropTest extends EmbeddedRestMemcachedHotRodTest {

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   @Test
   public void testRestPutEmbeddedMemcachedHotRodGetTest() throws Exception {
      final String key = "3";
      final Object value = "<hey>ho</hey>";
      final Marshaller marshaller = cacheFactory.getMarshaller();

      // 1. Put with REST
      byte[] bytes = marshaller.objectToByteBuffer(value);

      RestCacheClient restClient = cacheFactory.getRestCacheClient();
      RestResponse response = join(restClient.put(key, RestEntity.create(marshaller.mediaType(), bytes)));
      assertEquals(204, response.getStatus());

      // 2. Get with Embedded (given a marshaller, it can unmarshall the result)
      assertEquals(value, cacheFactory.getEmbeddedCache().get(key));

      // 3. Get with Memcached (given a marshaller, it can unmarshall the result)
      assertEquals(value, cacheFactory.getMemcachedClient().get(key));

      // 4. Get with Hot Rod
      assertEquals(value, cacheFactory.getHotRodCache().get(key));
   }
}
