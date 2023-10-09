package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.tracing.TracingClusterTest")
public class TracingClusterTest extends MultiHotRodServersTest {

   private static final int NUM_SERVERS = 3;

   // Configure OpenTelemetry SDK for tests
   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   @Override
   protected void modifyGlobalConfiguration(GlobalConfigurationBuilder builder) {
      builder.tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      config.tracing().cluster(true);
      createHotRodServers(NUM_SERVERS, config);
      waitForClusterToForm();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      telemetryClient.reset();
      super.destroy();
   }

   @Test
   public void spanExport() {
      RemoteCache<Object, Object> cache1 = client(0).getCache();
      cache1.put("AAA", "aaa");
      cache1.put("AAA", "bbb");
      cache1.put("AAA", "ccc");

      eventually(() -> telemetryClient.finishedSpanItems().toString(),
            () -> telemetryClient.finishedSpanItems().size() == 6, 10, TimeUnit.SECONDS);
      List<SpanData> spanItems = telemetryClient.finishedSpanItems();

      Map<String, List<SpanData>> spansByName = InMemoryTelemetryClient.aggregateByName(spanItems);
      assertThat(spansByName).hasSize(2);
      List<SpanData> parents = spansByName.get("PUT");
      assertThat(parents).hasSize(3);
      List<SpanData> children = spansByName.get("PutKeyValueCommand");
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
