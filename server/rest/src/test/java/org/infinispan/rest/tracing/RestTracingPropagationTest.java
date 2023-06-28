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
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "rest.tracing.RestTracingPropagationTest")
public class RestTracingPropagationTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "tracing";
   private static final String CLIENT_SPAN_NAME = "user-client-side-span";
   private static final String PUT_OPERATION_SPAN_NAME = "putValueToCache";

   // Configure OpenTelemetry SDK for tests
   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      global.tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);

      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager(global);
      cacheManager.createCache(CACHE_NAME, getDefaultClusteredCacheConfig(CacheMode.LOCAL).build());

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

      telemetryClient.withinClientSideSpan(CLIENT_SPAN_NAME, () -> {
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

      // Verify that the client span (user-client-side-span) and the two PUT server spans are exported correctly
      List<SpanData> allSpans = telemetryClient.finishedSpanItems();
      Map<String, List<SpanData>> spansByName = InMemoryTelemetryClient.aggregateByName(allSpans);

      Assertions.assertThat(spansByName).containsKeys(PUT_OPERATION_SPAN_NAME, CLIENT_SPAN_NAME);

      List<SpanData> clientSpans = spansByName.get(CLIENT_SPAN_NAME);
      Assertions.assertThat(clientSpans).hasSize(1);
      SpanData clientSpan = clientSpans.get(0);
      String clientTraceId = clientSpan.getTraceId();
      String clientSpanId = clientSpan.getSpanId();

      List<SpanData> serverSpans = spansByName.get(PUT_OPERATION_SPAN_NAME);
      Assertions.assertThat(serverSpans).hasSize(2).allSatisfy(spanData -> {
         // Verify server spans are correctly correlated to the client span
         Assertions.assertThat(spanData.getTraceId()).isEqualTo(clientTraceId);
         Assertions.assertThat(spanData.getParentSpanId()).isEqualTo(clientSpanId);

         Attributes attributes = spanData.getAttributes();
         Assertions.assertThat(attributes.get(AttributeKey.stringKey("cache"))).isEqualTo(CACHE_NAME);
         Assertions.assertThat(attributes.get(AttributeKey.stringKey("category")))
               .isEqualTo(SpanCategory.CONTAINER.toString());
      });
   }

   @Override
   protected void teardown() {
      try {
         telemetryClient.reset();
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
