package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestEventListener;
import org.infinispan.client.rest.RestResponseInfo;
import org.infinispan.client.rest.RestTaskClient.ResultType;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.ByRef;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.counter.configuration.CounterConfigurationSerializer;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.ResponseAssertion;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.infinispan.test.TestException;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestOperations {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testRestOperations(Protocol protocol) {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      builder.protocol(protocol);
      RestClient client = SERVERS.rest().withClientConfiguration(builder).create();
      RestCacheClient cache = client.cache(SERVERS.getMethodName());
      assertResponse(NO_CONTENT, cache.post("k1", "v1"), r -> assertEquals(protocol, r.protocol()));
      assertResponse(OK, cache.get("k1"), r -> {
         assertEquals(protocol, r.protocol());
         assertEquals("v1", r.body());
      });
      assertResponse(NO_CONTENT, cache.remove("k1"), r -> assertEquals(protocol, r.protocol()));
      assertResponse(NOT_FOUND, cache.get("k1"), r -> assertEquals(protocol, r.protocol()));
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
      String configJson = counterConfigToJson(config);
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
         ResponseAssertion.assertThat(client.cache("caching-listen").createWithConfiguration(RestEntity.create(MediaType.APPLICATION_JSON, "{\"distributed-cache\":{}}"))).isOk();

         sseListener.expectEvent("create-cache", "caching-listen");
         sseListener.expectEvent("lifecycle-event", "ISPN100002");
         sseListener.expectEvent("lifecycle-event", "ISPN100010");

         ResponseAssertion.assertThat(client.cache("caching-listen").delete()).isOk();
         sseListener.expectEvent("remove-cache", "caching-listen");
      }
   }

   public static class WeakSSEListener implements RestEventListener {
      private static final Log log = LogFactory.getLog(WeakSSEListener.class);
      protected static final Consumer<KeyValuePair<String, String>> NO_OP = ignore -> {};
      BlockingDeque<KeyValuePair<String, String>> events = new LinkedBlockingDeque<>();
      private final CountDownLatch openLatch;
      private final ConcurrentMap<String, List<String>> backup = new ConcurrentHashMap<>();

      public WeakSSEListener() {
         this.openLatch = new CountDownLatch(1);
      }

      @Override
      public void onOpen(RestResponseInfo response) {
         log.tracef("open");
         if (response.status() < 300) {
            openLatch.countDown();
         }
      }

      @Override
      public void onMessage(String id, String type, String data) {
         log.tracef("Received %s %s %s", id, type, data);
         this.events.add(new KeyValuePair<>(type, data));
      }

      public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
         return openLatch.await(timeout, unit);
      }

      public void expectEvent(String type, String subString) throws InterruptedException {
         expectEvent(type, subString, NO_OP);
      }

      public List<KeyValuePair<String, String>> poll(int num) throws InterruptedException {
         List<KeyValuePair<String, String>> polled = new ArrayList<>();

         for (int i = 0; i < num; i++) {
            KeyValuePair<String, String> event = events.poll(10, TimeUnit.SECONDS);
            assertNotNull(event);
            polled.add(event);
         }
         return polled;
      }

      public void expectEvent(String type, String subString, Consumer<KeyValuePair<String, String>> consumer) throws InterruptedException {
         CompletableFuture<KeyValuePair<String, String>> waitEvent = CompletableFuture.supplyAsync(() -> {
            ByRef<KeyValuePair<String, String>> pair = new ByRef<>(null);
            backup.computeIfPresent(type, (k, v) -> {
               int index = -1;
               for (int i = 0; i < v.size() && pair.get() == null; i++) {
                  if (v.get(i).contains(subString)) {
                     pair.set(new KeyValuePair<>(k, v.get(i)));
                     index = i;
                     break;
                  }
               }

               if (index >= 0) v.remove(index);
               return v;
            });

            while (pair.get() == null) {
               try {
                  KeyValuePair<String, String> event = events.poll(10, TimeUnit.SECONDS);
                  assert event != null : "No event received";

                  if (type.equals(event.getKey()) && event.getValue().contains(subString)) {
                     pair.set(event);
                     break;
                  } else {
                     backup.compute(event.getKey(), (k, v) -> {
                        if (v == null) v = new ArrayList<>();

                        v.add(event.getValue());
                        return v;
                     });
                  }
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new TestException(e);
               }
            }

            assert pair.get() != null : "Should contain event with: " + subString;
            return pair.get();
         });

         try {
            KeyValuePair<String, String> pair = waitEvent.get(10, TimeUnit.SECONDS);
            consumer.accept(pair);
         } catch (ExecutionException | TimeoutException e) {
            throw new TestException(e);
         }
      }
   }

   public static String counterConfigToJson(AbstractCounterConfiguration config) {
      org.infinispan.commons.io.StringBuilderWriter sw = new org.infinispan.commons.io.StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).build()) {
         new CounterConfigurationSerializer().serializeConfiguration(w, config);
      }
      return sw.toString();
   }

}
