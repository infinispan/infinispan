package org.infinispan.server.functional;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredTests.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testOperations() {
      RestClient client = SERVER_TEST.getRestClient(CacheMode.DIST_SYNC);
      String cache = SERVER_TEST.getMethodName();
      RestResponse response = sync(client.put(cache, "k1", "v1"));
      assertEquals(200, response.getStatus());
      response = sync(client.get(cache, "k1"));
      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
      response = sync(client.delete(cache, "k1"));
      assertEquals(200, response.getStatus());
      response = sync(client.get(cache, "k1"));
      assertEquals(404, response.getStatus());
   }
}
