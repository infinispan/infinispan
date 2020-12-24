package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Containers.ipAddress;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import io.jaegertracing.testcontainers.JaegerAllInOne;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Test OpenTracing integration with the Jaeger client
 *
 * We use the latest Jaeger client compatible with opentracing-api version 0.31.0,
 * as that's the only API version supported by both Jaeger and AppDynamics ATM.
 *
 * It's only possible to use the latest Jaeger client by replacing the opentracing-api jar
 * in the main {@code lib} directory (as opposed to the {@code server/lib} directory used by this test).
 *
 * @since 10.0
 */
public class RequestTracingIT {
   public static final String SERVICE_NAME = RequestTracingIT.class.getName();
   public static final int NUM_KEYS = 10;
   private static JaegerAllInOne JAEGER = new JaegerAllInOne("jaegertracing/all-in-one:latest");

   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
                                    .runMode(ServerRunMode.CONTAINER)
                                    .numServers(1)
                                    .mavenArtifacts("io.jaegertracing:jaeger-core:0.34.3",
                                                    "io.jaegertracing:jaeger-thrift:0.34.3",
                                                    "io.jaegertracing:jaeger-tracerresolver:0.34.3",
                                                    "io.opentracing.contrib:opentracing-tracerresolver:0.1.5",
                                                    "org.slf4j:slf4j-api:1.7.25",
                                                    "com.squareup.okhttp3:okhttp:3.14.4",
                                                    "com.squareup.okio:okio:1.13.0",
                                                    "org.apache.thrift:libthrift:0.13.0",
                                                    "io.opentracing:opentracing-api:0.31.0",
                                                    "io.opentracing:opentracing-util:0.31.0",
                                                    "io.opentracing:opentracing-noop:0.31.0",
                                                    "org.slf4j:slf4j-jdk14:1.7.5")
//                                    .mavenArtifacts("io.jaegertracing:jaeger-core:1.5.0",
//                                                    "io.jaegertracing:jaeger-thrift:1.5.0",
//                                                    "io.jaegertracing:jaeger-tracerresolver:1.5.0",
//                                                    "io.opentracing.contrib:opentracing-tracerresolver:0.1.8",
//                                                    "org.slf4j:slf4j-api:1.7.28",
//                                                    "com.squareup.okhttp3:okhttp:4.9.0",
//                                                    "com.squareup.okio:okio:2.8.0",
//                                                    "org.apache.thrift:libthrift:0.13.0",
//                                                    "io.opentracing:opentracing-api:0.33.0",
//                                                    "io.opentracing:opentracing-util:0.33.0",
//                                                    "io.opentracing:opentracing-noop:0.33.0",
//                                                    "org.slf4j:slf4j-jdk14:1.7.5")
                                    .property("infinispan.opentracing.factory.class", "io.jaegertracing.tracerresolver.internal.JaegerTracerFactory")
                                    .property("infinispan.opentracing.factory.method", "getTracer")
                                    .property("JAEGER_SERVICE_NAME", SERVICE_NAME)
                                    .property("JAEGER_SAMPLER_TYPE", "const")
                                    .property("JAEGER_SAMPLER_PARAM", "1")
                                    .property("JAEGER_REPORTER_LOG_SPANS", "true")
                                    .property("JAEGER_REPORTER_MAX_QUEUE_SIZE", "1")
                                    .property("JAEGER_REPORTER_FLUSH_INTERVAL", "1")
                                    .addListener(new InfinispanServerListener() {
                                       @Override
                                       public void before(InfinispanServerDriver driver) {
                                          JAEGER.start();
                                          String endpoint = String.format("http://%s:%s/api/traces",
                                                                          ipAddress(JAEGER),
                                                                          JaegerAllInOne.JAEGER_COLLECTOR_THRIFT_PORT);
                                          driver.getConfiguration().properties()
                                                .setProperty("JAEGER_ENDPOINT", endpoint);
                                       }

                                       @Override
                                       public void after(InfinispanServerDriver driver) {
                                          JAEGER.stop();
                                       }
                                    })
                                    .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);


   @Test
   public void testRequestIsTraced() {
      RemoteCache<Object, Object> remoteCache = SERVER_TEST.hotrod().create();
      for (int i = 0; i < NUM_KEYS; i++) {
         remoteCache.put("key" + i, "value");
      }

      OkHttpClient httpClient = new OkHttpClient();
      String queryUrl = String.format("http://%s:%s/api/traces?service=%s",
                                      ipAddress(JAEGER),
                                      JaegerAllInOne.JAEGER_QUERY_PORT,
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
