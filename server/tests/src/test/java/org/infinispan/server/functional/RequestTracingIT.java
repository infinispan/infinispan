package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Containers.ipAddress;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
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
   public static final String JAEGER_IMAGE = System.getProperty(TestSystemPropertyNames.JAEGER_IMAGE, "quay.io/jaegertracing/all-in-one:1.35.2");
   public static final String SERVICE_NAME = RequestTracingIT.class.getName();
   public static final int NUM_KEYS = 10;

   private static final GenericContainer JAEGER = new GenericContainer(JAEGER_IMAGE)
         .withEnv("COLLECTOR_OTLP_ENABLED", "true");

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .numServers(1)
               .property("infinispan.tracing.enabled", "true")
               .property("otel.traces.exporter", "otlp")
               .property("otel.service.name", SERVICE_NAME)
               .addListener(new InfinispanServerListener() {
                  @Override
                  public void before(InfinispanServerDriver driver) {
                     JAEGER.start();
                     String endpoint = String.format("http://%s:%s", ipAddress(JAEGER), "4317");
                     driver.getConfiguration().properties()
                           .setProperty("otel.exporter.otlp.endpoint", endpoint);
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

      OkHttpClient httpClient = new OkHttpClient();
      String queryUrl = String.format("http://%s:%s/api/traces?service=%s",
                                      ipAddress(JAEGER),
                                      JAEGER_QUERY_PORT,
                                      SERVICE_NAME);

      Eventually.eventually(() -> {
         try (Response response = httpClient.newCall(new Request.Builder().url(queryUrl).build()).execute()) {
            if (response.body() == null)
               return false;

            Json json = Json.read(response.body().string());
            return json.has("data") && !json.at("data").asList().isEmpty();
         }
      });
   }
}
