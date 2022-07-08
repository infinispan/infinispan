package org.infinispan.rest.tracing;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

      // Verify that the client span (user-client-side-span) and the two PUT server spans are exported correctly.
      // We're going now to correlate the client span with the server spans!
      List<SpanData> spans = inMemorySpanExporter.getFinishedSpanItems();
      Assertions.assertThat(spans).hasSize(3);

      String traceId = null;
      Set spanIds = new HashSet();
      Map<String, Integer> parentSpanIds = new HashMap<>();
      String parentSpan = null;

      for (SpanData span : spans) {
         if (traceId == null) {
            traceId = span.getTraceId();
         } else {
            // check that the spans have all the same trace id
            Assertions.assertThat(span.getTraceId()).isEqualTo(traceId);
         }

         spanIds.add(span.getSpanId());
         parentSpanIds.compute(span.getParentSpanId(), (key, value) -> (value == null) ? 1 : value + 1);

         Integer times = parentSpanIds.get(span.getParentSpanId());
         if (times == 2) {
            parentSpan = span.getParentSpanId();
         }
      }

      // we have 3 different spans:
      Assertions.assertThat(spanIds).hasSize(3);
      // two of which have the same parent span
      Assertions.assertThat(parentSpanIds).hasSize(2);
      // that is the other span
      Assertions.assertThat(spanIds).contains(parentSpan);
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
