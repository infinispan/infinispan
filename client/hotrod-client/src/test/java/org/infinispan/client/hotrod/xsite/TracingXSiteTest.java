package org.infinispan.client.hotrod.xsite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.telemetry.SpanCategory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.xsite.TracingXSiteTest")
public class TracingXSiteTest extends AbstractHotRodSiteFailoverTest {

   // Configure OpenTelemetry SDK for tests
   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   private RemoteCacheManager clientA;
   private RemoteCacheManager clientB;

   @Override
   protected void decorateGlobalConfiguration(GlobalConfigurationBuilder builder, int siteIndex, int nodeIndex) {
      builder.tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);
   }

   @Override
   protected void decorateCacheConfiguration(ConfigurationBuilder builder, int siteIndex, int nodeIndex) {
      builder.tracing().disableCategory(SpanCategory.CONTAINER);
      builder.tracing().disableCategory(SpanCategory.CLUSTER);
      builder.tracing().enableCategory(SpanCategory.X_SITE);
   }

   public void verifyXSiteEventsAreTraced() {
      clientA = client(SITE_A, Optional.of(SITE_B));
      clientB = client(SITE_B, Optional.empty());
      RemoteCache<Integer, String> cacheA = clientA.getCache(CACHE_NAME);
      RemoteCache<Integer, String> cacheB = clientB.getCache(CACHE_NAME);

      assertNull(cacheA.put(1, "v1"));
      assertEquals("v1", cacheA.get(1));
      assertEquals("v1", cacheB.get(1));
      assertEquals("v1", cacheB.get(1));
      assertEquals("v1", cacheA.get(1));

      eventually(() -> telemetryClient.finishedSpanItems().toString(),
            () -> telemetryClient.finishedSpanItems().size() == 1, 10, TimeUnit.SECONDS);
      List<SpanData> spanItems = telemetryClient.finishedSpanItems();

      assertThat(spanItems).hasSize(1);
      Attributes attributes = spanItems.get(0).getAttributes();
      assertThat(attributes.asMap())
            .hasSize(3)
            .containsEntry(AttributeKey.stringKey("cache"), cacheA.getName())
            .containsEntry(AttributeKey.stringKey("category"), SpanCategory.X_SITE.toString())
            .containsKey(AttributeKey.stringKey("server.address"));
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      HotRodClientTestingUtil.killRemoteCacheManagers(clientA, clientB);
      super.destroy();
   }
}
