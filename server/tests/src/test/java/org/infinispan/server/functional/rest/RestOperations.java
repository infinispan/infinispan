package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.server.test.core.Common.HTTP_PROTOCOLS;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient.ResultType;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.resources.AbstractRestResourceTest;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.logging.Log;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestOperations {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

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
      String configJson = AbstractRestResourceTest.counterConfigToJson(config);
      RestCounterClient counter = client.counter(SERVERS.getMethodName(protocol.name()));
      assertStatus(OK, counter.create(RestEntity.create(MediaType.APPLICATION_JSON, configJson)));

      assertEquals("5", assertStatus(OK, counter.get()));
   }

   @ParameterizedTest(name = "{0}-{1}")
   @ArgumentsSource(ArgsProvider.class)
   public void testSSECluster(Protocol protocol, AcceptSerialization serialization) throws Exception {
      var builder = new RestClientConfigurationBuilder().protocol(protocol);
      var client = SERVERS.rest().withClientConfiguration(builder).create();

      // backwards compatible events
      log.debug("Testing backwards compatible events");
      var url = "/rest/v2/container?action=listen";
      testEventsWith(Util.threadLocalRandomUUID().toString(), url, client, serialization, true, false, true, false);
      testEventsWith(Util.threadLocalRandomUUID().toString(), url + "&pretty=true", client, serialization, true, false, true, false);

      // all
      log.debug("Testing all events (no filter)");
      url = "/rest/v2/container?action=listen&category=all";
      testEventsWith(Util.threadLocalRandomUUID().toString(), url, client, serialization, true, true, true, true);
      testEventsWith(Util.threadLocalRandomUUID().toString(), url + "&pretty=true", client, serialization, true, true, true, true);

      for (var combination : Util.generatePowerSet(List.of("config", "lifecycle", "cluster", "security", "tasks", "cross-site"))) {
         log.debugf("Testing filter category=%s", String.join(",", combination));
         var config = combination.contains("config");
         var lifecycle = combination.contains("lifecycle");
         var cluster = combination.contains("cluster");
         var task = combination.contains("tasks");
         if (combination.isEmpty()) {
            // It falls back to config and lifecycle, to keep backwards compatibility.
            config = true;
            lifecycle = true;
         }
         url = "/rest/v2/container?action=listen&category=%s".formatted(String.join(",", combination));
         testEventsWith(Util.threadLocalRandomUUID().toString(), url, client, serialization, config, cluster, lifecycle, task);
         testEventsWith(Util.threadLocalRandomUUID().toString(), url + "&pretty=true", client, serialization, config, cluster, lifecycle, task);
      }
   }

   @ParameterizedTest(name = "{0}-{1}")
   @ArgumentsSource(ArgsProvider.class)
   public void testTasksSSEvents(Protocol protocol, AcceptSerialization serialization) throws Exception {
      Assumptions.assumeFalse(ClusteredIT.NATIVE_TESTING, "Server tasks do not work with native server");
      var builder = new RestClientConfigurationBuilder().protocol(protocol);
      var client = SERVERS.rest().withClientConfiguration(builder).create();

      // backwards compatible events
      log.warn("Testing backwards compatible events");
      var url = "/rest/v2/container?action=listen";
      testTasksEventsWith(url, client, serialization, false);
      testTasksEventsWith(url + "&pretty=true", client, serialization, false);

      // all
      log.warn("Testing all events (no filter)");
      url = "/rest/v2/container?action=listen&category=all";
      testTasksEventsWith(url, client, serialization, true);
      testTasksEventsWith(url + "&pretty=true", client, serialization, true);

      for (var combination : Util.generatePowerSet(List.of("config", "lifecycle", "cluster", "security", "tasks", "cross-site"))) {
         log.warnf("Testing filter category=%s", String.join(",", combination));
         var tasks = combination.contains("tasks");
         url = "/rest/v2/container?action=listen&category=%s".formatted(String.join(",", combination));
         testTasksEventsWith(url, client, serialization, tasks);
         log.warnf("Testing (pretty) filter category=%s", String.join(",", combination));
         testTasksEventsWith(url + "&pretty=true", client, serialization, tasks);
         log.warnf("Done testing filter category=%s", String.join(",", combination));
      }
   }

   private static void testTasksEventsWith(String url, RestClient client, AcceptSerialization serialization, boolean tasks) throws IOException, InterruptedException {
      var sseListener = new WeakSSEListener();
      try (Closeable ignored = client.raw().listen(url, Map.of("Accept", serialization.header()), sseListener)) {
         CompletionStage<RestResponse> response = client.tasks().exec("hello");
         ResponseAssertion.assertThat(response).isOk();

         if (tasks) {
            sseListener.expectEvent("tasks-event", "ISPN101000", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("tasks-event", Assertions::fail);
         }

         sseListener.expectNoEvent("create-cache", Assertions::fail);
         sseListener.expectNoEvent("remove-cache", Assertions::fail);
         sseListener.expectNoEvent("update-cache", Assertions::fail);
         sseListener.expectNoEvent("create-template", Assertions::fail);
         sseListener.expectNoEvent("remove-template", Assertions::fail);
         sseListener.expectNoEvent("update-template", Assertions::fail);
         sseListener.expectNoEvent("lifecycle-event", Assertions::fail);
         sseListener.expectNoEvent("security-event", Assertions::fail);
         sseListener.expectNoEvent("cluster-event", Assertions::fail);
         sseListener.expectNoEvent("cross-site-event", Assertions::fail);
      }
   }

   private static void testEventsWith(String cacheName, String url, RestClient client, AcceptSerialization serialization, boolean config, boolean cluster, boolean lifecycle, boolean tasks) throws IOException, InterruptedException {
      // security it no used anywhere
      // cross-site is not available in this configuration.
      var sseListener = new WeakSSEListener();
      try (Closeable ignored = client.raw().listen(url, Map.of("Accept", serialization.header()), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));
         assertThat(client.cache(cacheName).createWithTemplate("org.infinispan.DIST_SYNC")).isOk();

         if (config) {
            sseListener.expectEvent("create-cache", cacheName);
         } else {
            sseListener.expectNoEvent("create-cache", Assertions::fail);
         }
         if (cluster) {
            sseListener.expectEvent("cluster-event", "ISPN100002", serialization::checkEvent);
            sseListener.expectEvent("cluster-event", "ISPN100009", serialization::checkEvent);
            sseListener.expectEvent("cluster-event", "ISPN100010", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("cluster-event", Assertions::fail);
         }

         if (lifecycle) {
            sseListener.expectEvent("lifecycle-event", "ISPN100002", serialization::checkEvent);
            sseListener.expectEvent("lifecycle-event", "ISPN100010", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("lifecycle-event", Assertions::fail);
         }

         sseListener.expectNoEvent("tasks-event", Assertions::fail);
         sseListener.expectNoEvent("security-event", Assertions::fail);
         sseListener.expectNoEvent("cross-site-event", Assertions::fail);

         assertThat(client.cache(cacheName).delete()).isOk();

         if (config) {
            sseListener.expectEvent("remove-cache", cacheName);
         } else {
            sseListener.expectNoEvent("remove-cache", Assertions::fail);
         }

         if (cluster) {
            sseListener.expectEvent("cluster-event", "ISPN100008", serialization::checkEvent);
         } else {
            sseListener.expectNoEvent("cluster-event", Assertions::fail);
         }

         if (!lifecycle) {
            sseListener.expectNoEvent("lifecycle-event", Assertions::fail);
         }

         sseListener.expectNoEvent("tasks-event", Assertions::fail);
         sseListener.expectNoEvent("security-event", Assertions::fail);
         sseListener.expectNoEvent("cross-site-event", Assertions::fail);
      }
   }

   public static class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return HTTP_PROTOCOLS.stream()
               .flatMap(protocol ->
                     Arrays.stream(AcceptSerialization.values())
                           .map(serialization -> Arguments.of(protocol, serialization))
               );
      }
   }

   public enum AcceptSerialization {
      JSON {
         @Override
         public String header() {
            return MediaType.APPLICATION_JSON_TYPE;
         }

         @Override
         public boolean isAccepted(String s) {
            Json json = Json.read(s);
            if (!(json.isObject() && json.has("log"))) return false;

            Json log = json.at("log");
            if (!(log.has("content") && log.has("meta") && log.has("category"))) return false;

            Json content = log.at("content");
            Json meta = log.at("meta");
            return content.has("level") && content.has("message") && content.has("detail")
                  && meta.has("context") && meta.has("scope") && meta.has("who") && meta.has("instant");
         }
      },
      YAML {
         @Override
         public String header() {
            return MediaType.APPLICATION_YAML_TYPE;
         }

         @Override
         @SuppressWarnings("unchecked")
         public boolean isAccepted(String s) {
            Yaml yaml = new Yaml();
            LinkedHashMap<String, LinkedHashMap> ev = yaml.load(s);
            if (!(ev != null && ev.containsKey("log"))) return false;

            LinkedHashMap<String, LinkedHashMap> log = ev.get("log");
            if (!(log.containsKey("content") && log.containsKey("meta") && log.containsKey("category"))) return false;

            LinkedHashMap<String, String> content = log.get("content");
            LinkedHashMap<String, String> meta = log.get("meta");
            return content.containsKey("level") && content.containsKey("message") && content.containsKey("detail")
                  && meta.containsKey("context") && meta.containsKey("scope") && meta.containsKey("who") && meta.containsKey("instant");
         }
      },
      XML {
         @Override
         public String header() {
            return MediaType.APPLICATION_XML_TYPE;
         }

         @Override
         public boolean isAccepted(String content) {
            return content.startsWith("<?xml version=\"1.0\"?>")
                  && content.endsWith("</log>");
         }
      };

      public abstract String header();

      public abstract boolean isAccepted(String content);

      public void checkEvent(KeyValuePair<String, String> pair) {
         assertTrue(isAccepted(pair.getValue()), "Not a " + header() + ": " + pair.getValue());
      }
   }
}
