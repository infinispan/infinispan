package org.infinispan.server.insights;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.insights.config.InsightsActivation;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.redhat.insights.config.EnvAndSysPropsInsightsConfiguration;
import com.redhat.insights.reports.InsightsReport;

@Test(groups = "functional", testName = "server.insights.InsightsActivationTest")
public class InsightsActivationTest extends AbstractInfinispanTest {

   public void enabled() throws Exception {
      Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "blablabla");
      Files.createDirectories(tmpDir);
      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.ENABLED.name());
      System.setProperty(InfinispanInsightsConfiguration.ENV_RHEL_MACHINE_ID_FILE_PATH.toLowerCase().replace('_', '.'), tmpDir.toString());
      System.setProperty(EnvAndSysPropsInsightsConfiguration.ENV_ARCHIVE_UPLOAD_DIR.toLowerCase().replace('_', '.'), tmpDir.toString());
      try (EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager()) {
         InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         assertThat(service).isNotNull();
         InsightsReport report = service.report();
         String json = report.serialize();
         assertThat(json).isNotEmpty();
      } finally {
         System.clearProperty(InfinispanInsightsConfiguration.ENV_RHEL_MACHINE_ID_FILE_PATH.toLowerCase().replace('_', '.'));
         System.clearProperty(InfinispanInsightsConfiguration.ENV_ARCHIVE_UPLOAD_DIR.toLowerCase().replace('_', '.'));
      }
      eventually(() -> "report is produced locally", () -> Files.list(tmpDir).count() > 0);
   }

   public void notEnabled() throws Exception {
      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.LOCAL.name());
      try (EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager()) {
         InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         assertThat(service).isNotNull();
         InsightsReport report = service.report();
         String json = report.serialize();
         assertThat(json).isNotEmpty();
      }

      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.DISABLED.name());
      try (EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager()) {
         InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         assertThat(service).isNull();
      }
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void testClassFinished(ITestContext context) {
      System.clearProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME);
      super.testClassFinished(context);
   }
}
