package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient.ResultType;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class RestOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;
   private final Protocol protocol;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      List<Object[]> params = new ArrayList<>(HTTP_PROTOCOLS.size());
      for (Protocol protocol : HTTP_PROTOCOLS) {
         params.add(new Object[]{protocol});
      }
      return params;
   }

   public RestOperations(Protocol protocol) {
      this.protocol = protocol;
   }

   @Test
   public void testRestOperations() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVER_TEST.getMethodName());
      RestResponse response = sync(cache.put("k1", "v1"));
      assertEquals(204, response.getStatus());
      assertEquals(protocol, response.getProtocol());
      response = sync(cache.get("k1"));
      assertEquals(200, response.getStatus());
      assertEquals(protocol, response.getProtocol());
      assertEquals("v1", response.getBody());
      response = sync(cache.remove("k1"));
      assertEquals(204, response.getStatus());
      assertEquals(protocol, response.getProtocol());
      response = sync(cache.get("k1"));
      assertEquals(404, response.getStatus());
      assertEquals(protocol, response.getProtocol());
   }

   @Test
   public void testPutWithTimeToLive() throws InterruptedException {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVER_TEST.getMethodName());
      sync(cache.post("k1", "v1", 1, 1));
      assertEquals(HttpResponseStatus.OK.code(), sync(cache.get("k1")).getStatus());
      Thread.sleep(2000);
      assertEquals(HttpResponseStatus.NOT_FOUND.code(), sync(cache.get("k1")).getStatus());
   }


   @Test
   public void taskFilter() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();

      RestResponse tasks = sync(client.tasks().list(ResultType.USER));
      List<Json> taskListNode = Json.read(tasks.getBody()).asJsonList();

      taskListNode.forEach(n -> assertFalse(n.at("name").asString().startsWith("@@")));
   }
}
