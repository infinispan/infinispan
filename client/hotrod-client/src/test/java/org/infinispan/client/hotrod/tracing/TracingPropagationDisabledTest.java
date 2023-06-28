package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.model.Author;
import org.infinispan.client.hotrod.annotation.model.Poem;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.tracing.TracingPropagationDisabledTest")
public class TracingPropagationDisabledTest extends SingleHotRodServerTest {

   private static final String CLIENT_SPAN_NAME = "user-client-side-span";
   private static final String PUT_OPERATION_SPAN_NAME = "PUT";

   // Configure OpenTelemetry SDK for tests
   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      global.tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("poem.Poem");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager(global);
      manager.defineConfiguration("poems", builder.build());
      return manager;
   }

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = createHotRodClientConfigurationBuilder("127.0.0.1", hotrodServer.getPort());
      builder.disableTracingPropagation(); // <-- tracing context propagation is disabled client side
      return new InternalRemoteCacheManager(builder.build());
   }

   @Override
   protected void teardown() {
      telemetryClient.reset();
      super.teardown();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Poem.PoemSchema.INSTANCE;
   }

   @Test
   public void smokeTest() {
      RemoteCache<Integer, Poem> remoteCache = remoteCacheManager.getCache("poems");
      telemetryClient.withinClientSideSpan(CLIENT_SPAN_NAME, () -> {
         remoteCache.put(1, new Poem(new Author("Edgar Allen Poe"), "The Raven", 1845));
         remoteCache.put(2, new Poem(new Author("Emily Dickinson"), "Because I could not stop for Death", 1890));
      });

      // We might have a slight delay between receiving the response and the server span registered.
      eventually(() -> telemetryClient.finishedSpanItems().toString(),
            () -> telemetryClient.finishedSpanItems().size() == 3, 10, TimeUnit.SECONDS);

      // Verify that the client span (user-client-side-span) and the two PUT server spans are exported correctly
      List<SpanData> allSpans = telemetryClient.finishedSpanItems();
      Map<String, List<SpanData>> spansByName = InMemoryTelemetryClient.aggregateByName(allSpans);

      assertThat(spansByName).containsKeys(PUT_OPERATION_SPAN_NAME, CLIENT_SPAN_NAME);

      List<SpanData> clientSpans = spansByName.get(CLIENT_SPAN_NAME);
      assertThat(clientSpans).hasSize(1);
      SpanData clientSpan = clientSpans.get(0);
      String clientTraceId = clientSpan.getTraceId();
      String clientSpanId = clientSpan.getSpanId();

      List<SpanData> serverSpans = spansByName.get(PUT_OPERATION_SPAN_NAME);
      assertThat(serverSpans).hasSize(2).allSatisfy(spanData -> {
         // Verify server spans are NOT correctly correlated to the client span
         assertThat(spanData.getTraceId()).isNotEqualTo(clientTraceId);
         assertThat(spanData.getParentSpanId()).isNotEqualTo(clientSpanId);

         Attributes attributes = spanData.getAttributes();
         assertThat(attributes.get(AttributeKey.stringKey("cache"))).isEqualTo("poems");
         Assertions.assertThat(attributes.get(AttributeKey.stringKey("category")))
               .isEqualTo(SpanCategory.CONTAINER.toString());
      });
   }
}
