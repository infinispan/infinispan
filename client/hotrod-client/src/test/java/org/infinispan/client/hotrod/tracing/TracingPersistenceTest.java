package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.tracing.TracingPersistenceTest")
public class TracingPersistenceTest extends SingleHotRodServerTest {

   // Configure OpenTelemetry SDK for tests
   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   @Override
   protected void teardown() {
      telemetryClient.reset();
      super.teardown();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      telemetryClient.reset();
      assertThat(telemetryClient.finishedSpanItems()).isEmpty();

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      global.tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.tracing().enableCategory(SpanCategory.PERSISTENCE);
      builder.persistence()
            .passivation(false)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(true);

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager(global);
      manager.defineConfiguration("persistence", builder.build());
      return manager;
   }

   @Test
   public void test() throws Exception {
      RemoteCache<Integer, String> remoteCache = remoteCacheManager.getCache("persistence");
      remoteCache.put(1, "bla bla bla");
      remoteCache.put(2, "one two three");
      remoteCache.put(3, "foo bar baz");

      eventually(() -> telemetryClient.finishedSpanItems().toString(),
            () -> telemetryClient.finishedSpanItems().size() == 6, 10, TimeUnit.SECONDS);
      List<SpanData> spanItems = telemetryClient.finishedSpanItems();

      Map<String, List<SpanData>> spansByName = InMemoryTelemetryClient.aggregateByName(spanItems);
      List<SpanData> parents = spansByName.get("PUT");
      assertThat(parents).hasSize(3);
      List<SpanData> children = spansByName.get("writeToAllNonTxStores");
      assertThat(children).hasSize(3);

      Set<String> parentSpanIds = new HashSet<>(3);
      String rootId = null;
      for (SpanData parent : parents) {
         parentSpanIds.add(parent.getSpanId());
         if (rootId == null) {
            rootId = parent.getParentSpanId();
         } else {
            // verify that all parents have the same parent id (root id, e.g.: 00000000)
            assertThat(rootId).isEqualTo(parent.getParentSpanId());
         }
      }
      for (SpanData child : children) {
         String parentSpanId = child.getParentSpanId();
         assertThat(parentSpanIds.remove(parentSpanId)).isTrue();
      }
   }
}
