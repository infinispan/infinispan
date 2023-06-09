package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
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
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testRestOperations(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVERS.getMethodName());
      assertResponse(NO_CONTENT, cache.post("k1", "v1"), r -> assertEquals(protocol, r.getProtocol()));
      assertResponse(OK, cache.get("k1"), r -> {
         assertEquals(protocol, r.getProtocol());
         assertEquals("v1", r.getBody());
      });
      assertResponse(NO_CONTENT, cache.remove("k1"), r -> assertEquals(protocol, r.getProtocol()));
      assertResponse(NOT_FOUND, cache.get("k1"), r -> assertEquals(protocol, r.getProtocol()));
   }

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testPutWithTimeToLive(Protocol protocol) throws InterruptedException {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVERS.getMethodName());
      assertStatus(NO_CONTENT, cache.post("k1", "v1", 1, 1));
      assertStatus(OK, cache.get("k1"));
      Thread.sleep(2000);
      assertStatus(NOT_FOUND, cache.get("k1"));
   }


   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void taskFilter(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();

      List<Json> taskListNode = Json.read(assertStatus(OK, client.tasks().list(ResultType.USER))).asJsonList();
      taskListNode.forEach(n -> assertFalse(n.at("name").asString().startsWith("@@")));
   }

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testCounter(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();

      CounterConfiguration configuration = CounterConfiguration
            .builder(CounterType.WEAK)
            .initialValue(5)
            .concurrencyLevel(1)
            .build();

      AbstractCounterConfiguration config = ConvertUtil.configToParsedConfig("test-counter", configuration);
      String configJson = AbstractRestResourceTest.counterConfigToJson(config);
      RestCounterClient counter = client.counter(SERVERS.getMethodName(protocol.name()));
      assertStatus(OK, counter.create(RestEntity.create(MediaType.APPLICATION_JSON, configJson)));

      assertEquals("5", assertStatus(OK, counter.get()));
   }

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testSSECluster(Protocol protocol) throws Exception {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
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
