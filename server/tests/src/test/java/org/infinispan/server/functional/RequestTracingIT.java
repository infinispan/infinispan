package org.infinispan.server.functional;

import static org.assertj.core.api.Assertions.fail;
import static org.infinispan.server.test.core.Containers.ipAddress;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.telemetry.SpanCategory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;

/**
 * Test OpenTelemetry tracing integration with the Jaeger client
 *
 * @since 10.0
 */
public class RequestTracingIT {

   public static final int JAEGER_QUERY_PORT = 16686;
   public static final String JAEGER_IMAGE = System.getProperty(TestSystemPropertyNames.JAEGER_IMAGE, "quay.io/jaegertracing/all-in-one:1.46.0");
   public static final String SERVICE_NAME = "infinispan-server";

   private static final GenericContainer JAEGER = new GenericContainer(JAEGER_IMAGE)
         .withEnv("COLLECTOR_OTLP_ENABLED", "true");

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/BasicServerTest-withTracing.xml")
               .runMode(ServerRunMode.EMBEDDED)
               .numServers(2)
               .addListener(new InfinispanServerListener() {
                  @Override
                  public void before(InfinispanServerDriver driver) {
                     JAEGER.start();
                     String endpoint = String.format("http://%s:%s", ipAddress(JAEGER), "4318");
                     driver.getConfiguration().properties()
                           .setProperty("infinispan.tracing.collector-endpoint", endpoint);
                  }

                  @Override
                  public void after(InfinispanServerDriver driver) {
                     JAEGER.stop();
                  }
               })
               .build();

   @Test
   public void testRequestIsTraced() {
      RemoteCache<Object, Object> remoteCache = SERVER.hotrod().create();
      remoteCache.put("key", "value");
      String cacheName = remoteCache.getName();
      HttpClient client = HttpClient.newHttpClient();
      String queryUrl = String.format("http://%s:%s/api/traces?service=%s",
            ipAddress(JAEGER),
            JAEGER_QUERY_PORT,
            SERVICE_NAME);

      AtomicReference<List<Json>> returnedSpans = new AtomicReference<>();
      Eventually.eventually(() -> {
         HttpResponse<String> response = client.send(HttpRequest.newBuilder().uri(URI.create(queryUrl)).build(), HttpResponse.BodyHandlers.ofString());
         if (response.body() == null) {
            return false;
         }

         Json json = Json.read(response.body());
         if (!json.has("data")) {
            return false;
         }

         List<Json> data = json.at("data").asJsonList();
         if (data.isEmpty()) {
            return false;
         }

         List<Json> spans = data.get(0).asJsonMap().get("spans").asJsonList();
         if (spans.size() < 2) {
            return false;
         }

         returnedSpans.set(spans);
         return true;
      });

      List<Json> spans = returnedSpans.get();
      assertThat(spans.size()).isEqualTo(2).withFailMessage("Error on cleaning up pre-existing spans");
      Map<String, Json> span0 = spans.get(0).asJsonMap();
      Map<String, Json> span1 = spans.get(1).asJsonMap();

      Map<String, Json> hotRodPut = null;
      Map<String, Json> clusterPut = null;

      // verify the operation names
      String span0OperationName = span0.get("operationName").asString();
      String span1OperationName = span1.get("operationName").asString();
      if (span0OperationName.equals("PUT")) {
         assertThat(span1OperationName).isEqualTo("PutKeyValueCommand");

         hotRodPut = span0;
         clusterPut = span1;
      } else if (span0OperationName.equals("PutKeyValueCommand")) {
         assertThat(span1OperationName).isEqualTo("PUT");

         hotRodPut = span1;
         clusterPut = span0;
      } else {
         fail("Unexpected operationName: " + span0OperationName);
      }

      Map<String, Json> hotRodPutTags = hotRodPut.get("tags").asJsonList().stream().collect(Collectors.toMap(
            json -> json.asJsonMap().get("key").asString(),
            json -> json.asJsonMap().get("value")));
      Map<String, Json> clusterPutTags = clusterPut.get("tags").asJsonList().stream().collect(Collectors.toMap(
            json -> json.asJsonMap().get("key").asString(),
            json -> json.asJsonMap().get("value")));

      // verify if the cache
      assertThat(hotRodPutTags.get("cache").asString()).isEqualTo(cacheName);
      assertThat(clusterPutTags.get("cache").asString()).isEqualTo(cacheName);

      // verify the categories
      assertThat(hotRodPutTags.get("category").asString()).isEqualTo(SpanCategory.CONTAINER.toString());
      assertThat(clusterPutTags.get("category").asString()).isEqualTo(SpanCategory.CLUSTER.toString());

      // verify correlation
      String hotRodPutId = hotRodPut.get("spanID").asString();
      Map<String, Json> references = clusterPut.get("references").asJsonList().get(0).asJsonMap();
      assertThat(references.get("refType").asString()).isEqualTo("CHILD_OF");
      assertThat(references.get("spanID").asString()).isEqualTo(hotRodPutId);
   }
}
