package org.infinispan.server.insights;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import com.redhat.insights.InsightsReport;

@Test(groups = "functional", testName = "server.insights.BasicInsightsTest")
public class BasicInsightsTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager();
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      config.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(config.persistence()));
      manager.createCache("blablabla", config.build());
      return manager;
   }

   public void test() {
      InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
      InsightsReport report = service.report();
      String json = report.serialize();
      assertThat(json).isNotEmpty();

      // verify some parsing results
      Json parsed = Json.read(json);
      assertThat(parsed.isObject()).isTrue();
      Json infinispan = parsed.at("infinispan");
      assertThat(infinispan.isObject()).isTrue();
      assertThat(infinispan.at("cluster-size").isNumber()).isTrue();
      assertThat(infinispan.at("cache-features").asMap()).containsExactly(entry("persistence", 1L));
      assertThat(infinispan.at("cache-stores").asList()).containsExactly("dummy-store");
   }
}
