package org.infinispan.rest.tracing;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.server.core.telemetry.TelemetryService;
import org.infinispan.server.core.telemetry.impl.OpenTelemetryService;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "functional", testName = "rest.tracing.TracingPropagationTest")
public class TracingPropagationTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "tracing";

   private final InMemorySpanExporter inMemorySpanExporter = InMemorySpanExporter.create();

   // Configure OpenTelemetry SDK for tests
   private final OpenTelemetryClient oTelConfig = new OpenTelemetryClient(inMemorySpanExporter);

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();
      cacheManager.createCache(CACHE_NAME, getDefaultClusteredCacheConfig(CacheMode.LOCAL).build());

      GlobalComponentRegistry globalComponentRegistry = cacheManager.getGlobalComponentRegistry();
      globalComponentRegistry.registerComponent(new OpenTelemetryService(oTelConfig.openTelemetry()), TelemetryService.class);

      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort())
            .build());

      return cacheManager;
   }

   @Test
   public void smokeTest() {
      RestCacheClient client = restClient.cache(CACHE_NAME);

      oTelConfig.withinClientSideSpan("user-client-side-span", () -> {
         // verify that the client thread contains the span context
         Map<String, String> contextMap = getContextMap();
         Assertions.assertThat(contextMap).isNotEmpty();

         CompletionStage<RestResponse> resp1 = client.put("aaa", MediaType.TEXT_PLAIN.toString(),
               RestEntity.create(MediaType.TEXT_PLAIN, "bbb"), contextMap);
         CompletionStage<RestResponse> resp2 = client.put("bbb", MediaType.TEXT_PLAIN.toString(),
               RestEntity.create(MediaType.TEXT_PLAIN, "ccc"), contextMap);

         assertThat(resp1).isOk();
         assertThat(resp2).isOk();
      });

      List<SpanData> spans = inMemorySpanExporter.getFinishedSpanItems();

      // TODO Produce server side (Rest) tracing spans and correlate them with this one
      Assertions.assertThat(spans).hasSize(1);
   }

   @Override
   protected void teardown() {
      try {
         oTelConfig.shutdown();
         restClient.close();
      } catch (IOException ex) {
         // ignore it
      } finally {
         try {
            restServer.stop();
         } finally {
            super.teardown();
         }
      }
   }

   public static Map<String, String> getContextMap() {
      HashMap<String, String> result = new HashMap<>();

      // Inject the request with the *current* Context, which contains our current Span.
      W3CTraceContextPropagator.getInstance().inject(Context.current(), result,
            (carrier, key, value) -> carrier.put(key, value));
      return result;
   }
}
