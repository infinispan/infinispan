package org.infinispan.server.functional.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Tag("embedded")
public class OverviewReportIT {

   public static final String CACHE_NAME = "blablabla";

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(2)
               .build();

   @Test
   @Tag("embedded")
   public void testExample() {
      // use HotRod
      RemoteCacheManager cacheManager = SERVERS.hotrod().createRemoteCacheManager();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<Object, Object> cache = cacheManager.administration().getOrCreateCache(CACHE_NAME, builder.build());
      cache.put("ciao", "ciao");

      // use REST
      RestClient restClient = SERVERS.rest().get();
      join(restClient.cache(CACHE_NAME).put("ok", "ok"));

      CompletionStage<RestResponse> response = restClient.server().overviewReport();
      ResponseAssertion.assertThat(response).isOk();
      Json report = Json.read(join(response).getBody());

      Json security = report.at("security");
      assertThat(security.at("security-realms").at("default").at("tls").asString()).isEqualTo("NONE");
      assertThat(security.at("tls-endpoints").asJsonList()).isEmpty();
   }
}
