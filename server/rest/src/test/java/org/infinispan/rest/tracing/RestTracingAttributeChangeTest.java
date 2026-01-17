package org.infinispan.rest.tracing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.testing.Testing.tmpDirectory;

import java.nio.file.Paths;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.util.common.impl.Closer;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.server.core.telemetry.TelemetryServiceFactory;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryClient;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.TestResourceTracker;
import org.testng.annotations.Test;

public class RestTracingAttributeChangeTest extends SingleCacheManagerTest {

   public static final String CACHE_NAME = "bla";

   public static final String CACHE_DEFINITION =
      """
         <?xml version="1.0"?>
         <local-cache name="bla" statistics="true">
           <encoding media-type="application/x-protostream"/>
         </local-cache>""";

   private static final String PERSISTENT_LOCATION = tmpDirectory(RestTracingRuntimeEnablingTest.class.getName());

   private final InMemoryTelemetryClient telemetryClient = new InMemoryTelemetryClient();

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager(globalConfiguration());
      restServer = new RestServerHelper(manager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort())
            .build());

      return manager;
   }

   @Override
   protected void teardown() {
      try (Closer<Exception> closer = new Closer<>()) {
         closer.push(InMemoryTelemetryClient::reset, telemetryClient);
         closer.push(RestClient::close, restClient);
         closer.push(RestServerHelper::stop, restServer);
      } catch (Exception e) {
         // ignore it
      } finally {
         Util.recursiveFileRemove(PERSISTENT_LOCATION);
         super.teardown();
      }
   }

   @Test
   public void tryToChangeTracing() {
      RestEntity config = RestEntity.create(APPLICATION_XML, CACHE_DEFINITION);
      RestCacheClient restCache = restClient.cache(CACHE_NAME);
      CompletionStage<RestResponse> createCache = restCache.createWithConfiguration(config);
      ResponseAssertion.assertThat(createCache).isOk();

      CompletionStage<RestResponse> enabled = restCache.updateConfigurationAttribute("tracing.enabled", "true");
      ResponseAssertion.assertThat(enabled).isOk();
      CompletionStage<RestResponse> categories = restCache.updateConfigurationAttribute("tracing.categories", "cluster x-site");
      ResponseAssertion.assertThat(categories).isOk();

      Configuration configuration = cacheManager.getCacheConfiguration(CACHE_NAME);
      assertThat(configuration.tracing().categories())
            .containsExactlyInAnyOrder(SpanCategory.X_SITE, SpanCategory.CLUSTER);
   }

   private GlobalConfigurationBuilder globalConfiguration() {
      Util.recursiveFileRemove(PERSISTENT_LOCATION);
      GlobalConfigurationBuilder config = new GlobalConfigurationBuilder().nonClusteredDefault();
      config.globalState().enable()
            .configurationStorage(ConfigurationStorage.OVERLAY)
            .persistentLocation(Paths.get(PERSISTENT_LOCATION).toString())
            .metrics().accurateSize(true)
            .tracing().collectorEndpoint(TelemetryServiceFactory.IN_MEMORY_COLLECTOR_ENDPOINT);
      return config;
   }
}
