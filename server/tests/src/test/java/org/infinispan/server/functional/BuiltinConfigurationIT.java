package org.infinispan.server.functional;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class BuiltinConfigurationIT {

   @ClassRule
   public static InfinispanServerRule SERVERS =  InfinispanServerRuleBuilder.config("-")
         .runMode(ServerRunMode.EMBEDDED)
         .numServers(2)
         .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testHotRodOperations() {
      RemoteCache<String, String> cache = SERVER_TEST.hotrod().create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
      cache.remove("k1");
      assertEquals(0, cache.size());
   }

   @Test
   public void testRestOperations() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVER_TEST.getMethodName());
      RestResponse response = sync(cache.put("k1", "v1"));
      assertEquals(204, response.getStatus());
      response = sync(cache.get("k1"));
      assertEquals(200, response.getStatus());
      assertEquals("v1", response.getBody());
      response = sync(cache.remove("k1"));
      assertEquals(204, response.getStatus());
      response = sync(cache.get("k1"));
      assertEquals(404, response.getStatus());
   }
}
