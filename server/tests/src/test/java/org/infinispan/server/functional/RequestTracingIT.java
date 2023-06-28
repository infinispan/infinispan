package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Containers.ipAddress;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Test OpenTelemetry tracing integration with the Jaeger client
 *
 * @since 10.0
 */
public class RequestTracingIT {

   public static final int JAEGER_QUERY_PORT = 16686;
   public static final String JAEGER_IMAGE = System.getProperty(TestSystemPropertyNames.JAEGER_IMAGE, "quay.io/jaegertracing/all-in-one:1.46.0");
   public static final String SERVICE_NAME = "infinispan-server";
   public static final int NUM_KEYS = 10;

   private static final GenericContainer JAEGER = new GenericContainer(JAEGER_IMAGE)
         .withEnv("COLLECTOR_OTLP_ENABLED", "true");

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/BasicServerTest-withTracing.xml")
               .runMode(ServerRunMode.EMBEDDED)
               .numServers(1)
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
      for (int i = 0; i < NUM_KEYS; i++) {
         remoteCache.put("key" + i, "value");
      }

      String cacheName = remoteCache.getName();

      OkHttpClient httpClient = new OkHttpClient();
      String queryUrl = String.format("http://%s:%s/api/traces?service=%s",
                                      ipAddress(JAEGER),
                                      JAEGER_QUERY_PORT,
                                      SERVICE_NAME);

      AtomicReference<List<Json>> returnedTraces = new AtomicReference<>();
      Eventually.eventually(() -> {
         try (Response response = httpClient.newCall(new Request.Builder().url(queryUrl).build()).execute()) {
            if (response.body() == null) {
               return false;
            }

            Json json = Json.read(response.body().string());
            if (!json.has("data")) {
               return false;
            }

            List<Json> traces = json.at("data").asJsonList();
            returnedTraces.set(traces);
            return !traces.isEmpty();
         }
      });

      Map<String, Json> span = returnedTraces.get().get(0).asJsonMap().get("spans").asJsonList().get(0).asJsonMap();
      assertThat(span.get("operationName").asString()).isEqualTo("PUT");

      Map<String, Json> tags = span.get("tags").asJsonList().stream().collect(Collectors.toMap(
            json -> json.asJsonMap().get("key").asString(),
            json -> json.asJsonMap().get("value")));

      assertThat(tags.get("cache").asString()).isEqualTo(cacheName);
      assertThat(tags.get("category").asString()).isEqualTo(SpanCategory.CONTAINER.toString());
   }
}
