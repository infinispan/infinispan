package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.model.Author;
import org.infinispan.client.hotrod.annotation.model.Poem;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;

@Test(groups = "functional", testName = "org.infinispan.client.hotrod.tracing.TracingPropagationTest")
public class TracingPropagationTest extends SingleHotRodServerTest {

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
      globalComponentRegistry.registerComponent(oTelConfig.openTelemetry(), OpenTelemetry.class);
      return manager;
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
         // verify that the client thread contains the span context
         Map<String, String> contextMap = getContextMap();
         assertThat(contextMap).isNotEmpty();

         remoteCache.put(1, new Poem(new Author("Edgar Allen Poe"), "The Raven", 1845));
         remoteCache.put(2, new Poem(new Author("Emily Dickinson"), "Because I could not stop for Death", 1890));
      });

      // Verify that the client span (user-client-side-span) and the two PUT server spans are exported correctly.
      // We're going now to correlate the client span with the server spans!
      assertThat(inMemorySpanExporter.getFinishedSpanItems()).hasSize(3);
   }

   public static Map<String, String> getContextMap() {
      HashMap<String, String> result = new HashMap<>();

      // Inject the request with the *current* Context, which contains our current Span.
      W3CTraceContextPropagator.getInstance().inject(Context.current(), result,
            (carrier, key, value) -> carrier.put(key, value));
      return result;
   }
}
