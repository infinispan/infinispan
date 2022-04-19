package org.infinispan.server.functional;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestTaskClient.ResultType;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.rest.resources.AbstractRestResourceTest;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
      assertResponse(NO_CONTENT, cache.post("k1", "v1"), r -> assertEquals(protocol, r.getProtocol()));
      assertResponse(OK, cache.get("k1"), r -> {
         assertEquals(protocol, r.getProtocol());
         assertEquals("v1", r.getBody());
      });
      assertResponse(NO_CONTENT, cache.remove("k1"), r -> assertEquals(protocol, r.getProtocol()));
      assertResponse(NOT_FOUND, cache.get("k1"), r -> assertEquals(protocol, r.getProtocol()));
   }

   @Test
   public void testPutWithTimeToLive() throws InterruptedException {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVER_TEST.getMethodName());
      assertStatus(NO_CONTENT, cache.post("k1", "v1", 1, 1));
      assertStatus(OK, cache.get("k1"));
      Thread.sleep(2000);
      assertStatus(NOT_FOUND, cache.get("k1"));
   }


   @Test
   public void taskFilter() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();

      List<Json> taskListNode = Json.read(assertStatus(OK, client.tasks().list(ResultType.USER))).asJsonList();
      taskListNode.forEach(n -> assertFalse(n.at("name").asString().startsWith("@@")));
   }

   @Test
   public void testCounter() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();

      CounterConfiguration configuration = CounterConfiguration
            .builder(CounterType.WEAK)
            .initialValue(5)
            .concurrencyLevel(1)
            .build();

      AbstractCounterConfiguration config = ConvertUtil.configToParsedConfig("test-counter", configuration);
      String configJson = AbstractRestResourceTest.counterConfigToJson(config);
      RestCounterClient counter = client.counter("test");
      assertStatus(OK, counter.create(RestEntity.create(MediaType.APPLICATION_JSON, configJson)));

      assertEquals("5", assertStatus(OK, counter.get()));
   }

   @Test
   public void testSSECluster() throws Exception {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      WeakSSEListener sseListener = new WeakSSEListener();

      try (Closeable ignored = client.raw().listen("/rest/v2/container?action=listen", Collections.emptyMap(), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));

         assertThat(client.cache("caching-listen").createWithTemplate("org.infinispan.DIST_SYNC")).isOk();

         sseListener.expectEvent("create-cache", "caching-listen");
         sseListener.expectEvent("lifecycle-event", "ISPN100002");
         sseListener.expectEvent("lifecycle-event", "ISPN100010");

         assertThat(client.cache("caching-listen").delete()).isOk();
         sseListener.expectEvent("remove-cache", "caching-listen");
      }
   }
}
