package org.infinispan.client.hotrod.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "org.infinispan.client.hotrod.tracing.TracingRuntimeEnablingTest")
public class TracingRuntimeEnablingTest extends SingleHotRodServerTest {

   private static final String CACHE_A = "cacheA";
   private static final String CACHE_B = "cacheB";

   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      telemetryClient.reset();
      assertThat(telemetryClient.finishedSpanItems()).isEmpty();

      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder().nonClusteredDefault();
      global.tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);

      ConfigurationBuilder configA = new ConfigurationBuilder();
      configA.tracing().enable();
      ConfigurationBuilder configB = new ConfigurationBuilder();
      configB.tracing().disable();

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager(global);
      manager.defineConfiguration(CACHE_A, configA.build());
      manager.defineConfiguration(CACHE_B, configB.build());
      return manager;
   }

   /**
    * Configure the server, enabling the admin operations
    *
    * @return the HotRod server
    */
   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

      return HotRodClientTestingUtil.startHotRodServer(cacheManager, serverBuilder);
   }

   @Override
   protected void teardown() {
      telemetryClient.reset();
      super.teardown();
   }

   @Test
   public void test() {
      RemoteCache<Object, Object> cacheA = remoteCacheManager.getCache(CACHE_A);
      RemoteCache<Object, Object> cacheB = remoteCacheManager.getCache(CACHE_B);

      cacheA.put("AAA", "BBB");
      cacheB.put("BBB", "CCC");
      eventuallyEquals(1, () -> telemetryClient.finishedSpanItems().size());

      List<SpanData> result = telemetryClient.finishedSpanItems();
      SpanData span = result.get(0);
      assertThat(span.getName()).isEqualTo("PUT");

      Attributes attributes = span.getAttributes();
      assertThat(attributes.get(AttributeKey.stringKey("cache"))).isEqualTo(CACHE_A);
      assertThat(attributes.get(AttributeKey.stringKey("category"))).isEqualTo(SpanCategory.CONTAINER.toString());

      telemetryClient.reset();

      remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .updateConfigurationAttribute(CACHE_A, "tracing.enabled", "false");
      remoteCacheManager.administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
            .updateConfigurationAttribute(CACHE_B, "tracing.enabled", "true");

      cacheA.put("CCC", "DDD");
      cacheB.put("DDD", "EEE");
      eventuallyEquals(1, () -> telemetryClient.finishedSpanItems().size());

      result = telemetryClient.finishedSpanItems();
      span = result.get(0);
      assertThat(span.getName()).isEqualTo("PUT");

      attributes = span.getAttributes();
      assertThat(attributes.get(AttributeKey.stringKey("cache"))).isEqualTo(CACHE_B);
      assertThat(attributes.get(AttributeKey.stringKey("category"))).isEqualTo(SpanCategory.CONTAINER.toString());
   }
}
