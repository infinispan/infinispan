package org.infinispan.server.insights;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.infinispan.test.AbstractCacheTest.getDefaultClusteredCacheConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.server.insights.config.InsightsActivation;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.redhat.insights.reports.InsightsReport;

@Test(groups = "functional", testName = "server.insights.InsightsActivationTest", dataProvider = "optOut")
public class InsightsActivationTest extends AbstractInfinispanTest {

   private static final String RHEL_MACHINE_ID_FILE_PATH
         = InfinispanInsightsConfiguration.ENV_RHEL_MACHINE_ID_FILE_PATH.toLowerCase().replace('_', '.');
   private static final String ARCHIVE_UPLOAD_DIR
         = InfinispanInsightsConfiguration.ENV_ARCHIVE_UPLOAD_DIR.toLowerCase().replace('_', '.');
   private static final String OPT_OUT
         = InfinispanInsightsConfiguration.ENV_OPT_OUT.toLowerCase().replace('_', '.');

   public void enabled(boolean optOut) throws Exception {
      Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "blablabla");
      Files.createDirectories(tmpDir);
      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.ENABLED.name());
      System.setProperty(RHEL_MACHINE_ID_FILE_PATH, tmpDir.toString());
      System.setProperty(ARCHIVE_UPLOAD_DIR, tmpDir.toString());
      System.setProperty(OPT_OUT, optOut + "");
      try (EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager()) {
         InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         assertThat(service).isNotNull();
         try (InsightsReport report = service.report()) {
            assertThat(report).isNotNull();
         }
         // Don't serialize the report here to avoid conflicts with the concurrent serialization made by the Insights client

         if (!optOut) {
            // with this option the Insights Java client (not our code) won't
            // produce the report even on the local filesystem
            eventually(() -> "report is produced locally", () -> {
               try (Stream<Path> list = Files.list(tmpDir)) {
                  long count = list.count();
                  return count > 0;
               }
            });
         }
      } finally {
         System.clearProperty(RHEL_MACHINE_ID_FILE_PATH);
         System.clearProperty(ARCHIVE_UPLOAD_DIR);
         System.clearProperty(OPT_OUT);
      }
   }

   public void locallyEnabled(boolean optOut) throws Exception {
      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.LOCAL.name());
      System.setProperty(OPT_OUT, optOut + "");
      try (EmbeddedCacheManager manager = TestCacheManagerFactory.createServerModeCacheManager()) {
         ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
         config.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(config.persistence()));
         manager.createCache("blablabla", config.build());

         InsightsService service = manager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         try (InsightsReport report = service.report()) {
            String json = report.serialize();
            assertThat(report).isNotNull();
            // verify some parsing results
            Json parsed = Json.read(json);
            assertThat(parsed.isObject()).isTrue();
            Json infinispan = parsed.at("infinispan");
            assertThat(infinispan.isObject()).isTrue();
            assertThat(infinispan.at("cluster-size").isNumber()).isTrue();
            assertThat(infinispan.at("cache-features").asMap()).containsExactly(entry("persistence", 1L));
            assertThat(infinispan.at("cache-stores").asList()).containsExactly("DummyInMemoryStoreConfiguration");
         }
      } finally {
         System.clearProperty(OPT_OUT);
      }
   }

   public void notEnabled(boolean optOut) throws Exception {
      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.DISABLED.name());
      System.setProperty(OPT_OUT, optOut + "");
      try (EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager()) {
         InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         assertThat(service).isNull();
      } finally {
         System.clearProperty(OPT_OUT);
      }
   }

   @DataProvider(name = "optOut")
   public Object[][] optOutOptions() {
      return new Object[][]{{true}, {false}};
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void testClassFinished(ITestContext context) {
      System.clearProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME);
      super.testClassFinished(context);
   }
}
