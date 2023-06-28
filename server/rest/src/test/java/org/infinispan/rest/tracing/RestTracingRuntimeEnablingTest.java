package org.infinispan.rest.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.util.common.impl.Closer;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;

@Test(groups = "tracing", testName = "rest.tracing.RestTracingRuntimeEnablingTest")
public class RestTracingRuntimeEnablingTest extends SingleCacheManagerTest {

   private static final String CACHE_A = "cacheA";
   private static final String CACHE_B = "cacheB";
   private static final String PUT_OPERATION_SPAN_NAME = "putValueToCache";

   private static final String PERSISTENT_LOCATION = tmpDirectory(RestTracingRuntimeEnablingTest.class.getName());

   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);

      ConfigurationBuilder configA = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      configA.tracing().enable();
      ConfigurationBuilder configB = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      configB.tracing().disable();

      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager(globalConfiguration());
      manager.administration().createCache(CACHE_A, configA.build());
      manager.administration().createCache(CACHE_B, configB.build());

      restServer = new RestServerHelper(manager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort())
            .build());

      return manager;
   }

   @Test
   public void smokeTest() {
      RestCacheClient cacheA = restClient.cache(CACHE_A);
      RestCacheClient cacheB = restClient.cache(CACHE_B);

      CompletionStage<RestResponse> resp1 = cacheA.put("aaa", MediaType.TEXT_PLAIN.toString(),
            RestEntity.create(MediaType.TEXT_PLAIN, "bbb"));
      CompletionStage<RestResponse> resp2 = cacheB.put("bbb", MediaType.TEXT_PLAIN.toString(),
            RestEntity.create(MediaType.TEXT_PLAIN, "ccc"));

      ResponseAssertion.assertThat(resp1).isOk();
      ResponseAssertion.assertThat(resp2).isOk();

      eventuallyEquals(1, () -> telemetryClient.finishedSpanItems().size());

      List<SpanData> result = telemetryClient.finishedSpanItems();
      SpanData span = result.get(0);
      assertThat(span.getName()).isEqualTo(PUT_OPERATION_SPAN_NAME);

      Attributes attributes = span.getAttributes();
      assertThat(attributes.get(AttributeKey.stringKey("cache"))).isEqualTo(CACHE_A);
      assertThat(attributes.get(AttributeKey.stringKey("category"))).isEqualTo(SpanCategory.CONTAINER.toString());

      telemetryClient.reset();

      resp1 = cacheA.updateConfigurationAttribute("tracing.enabled", "false");
      resp2 = cacheB.updateConfigurationAttribute("tracing.enabled", "true");
      ResponseAssertion.assertThat(resp1).isOk();
      ResponseAssertion.assertThat(resp2).isOk();

      eventuallyEquals(false, () -> cacheManager.getCache(CACHE_A).getCacheConfiguration().tracing().enabled());
      eventuallyEquals(true, () -> cacheManager.getCache(CACHE_B).getCacheConfiguration().tracing().enabled());

      resp1 = cacheA.put("ccc", MediaType.TEXT_PLAIN.toString(), RestEntity.create(MediaType.TEXT_PLAIN, "ddd"));
      resp2 = cacheB.put("ddd", MediaType.TEXT_PLAIN.toString(), RestEntity.create(MediaType.TEXT_PLAIN, "eee"));
      ResponseAssertion.assertThat(resp1).isOk();
      ResponseAssertion.assertThat(resp2).isOk();

      eventuallyEquals(1, () -> telemetryClient.finishedSpanItems().size());

      result = telemetryClient.finishedSpanItems();
      span = result.get(0);
      assertThat(span.getName()).isEqualTo(PUT_OPERATION_SPAN_NAME);

      attributes = span.getAttributes();
      assertThat(attributes.get(AttributeKey.stringKey("cache"))).isEqualTo(CACHE_B);
      assertThat(attributes.get(AttributeKey.stringKey("category"))).isEqualTo(SpanCategory.CONTAINER.toString());
   }

   @Override
   protected void teardown() {
      try (Closer<IOException> closer = new Closer<>()) {
         closer.push(InMemoryTelemetryClient::reset, telemetryClient);
         closer.push(RestClient::close, restClient);
         closer.push(RestServerHelper::stop, restServer);
      } catch (IOException e) {
         // ignore it
      } finally {
         Util.recursiveFileRemove(PERSISTENT_LOCATION);
         super.teardown();
      }
   }

   private GlobalConfigurationBuilder globalConfiguration() {
      GlobalConfigurationBuilder config = new GlobalConfigurationBuilder().nonClusteredDefault();
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION).toString())
            .metrics().accurateSize(true)
            .tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);
      return config;
   }
}
