package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.annotation.model.Author;
import org.infinispan.client.hotrod.annotation.model.Poem;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.server.core.telemetry.TelemetryService;
import org.infinispan.server.core.telemetry.impl.OpenTelemetryService;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.tracing.TracingDisabledTest")
public class TracingDisabledTest extends SingleHotRodServerTest {

   private final InMemorySpanExporter inMemorySpanExporter = InMemorySpanExporter.create();

   // Configure OpenTelemetry SDK for tests
   private final OpenTelemetryClient oTelConfig = new OpenTelemetryClient(inMemorySpanExporter);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("poem.Poem");

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      manager.defineConfiguration("poems", builder.build());

      GlobalComponentRegistry globalComponentRegistry = manager.getGlobalComponentRegistry();
      globalComponentRegistry.registerComponent(new OpenTelemetryService(oTelConfig.openTelemetry()), TelemetryService.class);
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
      oTelConfig.shutdown();
      super.teardown();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return Poem.PoemSchema.INSTANCE;
   }

   @Test
   public void smokeTest() {
      RemoteCache<Integer, Poem> remoteCache = remoteCacheManager.getCache("poems");
      oTelConfig.withinClientSideSpan("user-client-side-span", () -> {
         remoteCache.put(1, new Poem(new Author("Edgar Allen Poe"), "The Raven", 1845));
         remoteCache.put(2, new Poem(new Author("Emily Dickinson"), "Because I could not stop for Death", 1890));
      });

      // We might have a slight delay between receiving the response and the server span registered.
      eventually(() -> inMemorySpanExporter.getFinishedSpanItems().toString(),
            () -> inMemorySpanExporter.getFinishedSpanItems().size() == 3, 10, TimeUnit.SECONDS);

      // Verify that the client span (user-client-side-span) and the two PUT server spans are exported correctly.
      // We're going now to correlate the client span with the server spans!
      List<SpanData> spans = inMemorySpanExporter.getFinishedSpanItems();
      assertThat(spans).hasSize(3);

      String traceId = null;
      Set spanIds = new HashSet();
      Map<String, Integer> parentSpanIds = new HashMap<>();
      String parentSpan = null;

      for (SpanData span : spans) {
         if (traceId == null) {
            traceId = span.getTraceId();
         } else {
            // check that the spans have all different trace ids
            assertThat(span.getTraceId()).isNotEqualTo(traceId);
         }

         spanIds.add(span.getSpanId());
         parentSpanIds.compute(span.getParentSpanId(), (key, value) -> (value == null) ? 1 : value + 1);

         Integer times = parentSpanIds.get(span.getParentSpanId());
         if (times == 2) {
            parentSpan = span.getParentSpanId();
         }
      }

      // we have 3 different spans:
      assertThat(spanIds).hasSize(3);
      // client spans <--> server spans are not correlated
      assertThat(parentSpanIds).hasSize(1);
      assertThat(spanIds).doesNotContain(parentSpan);
   }
}
