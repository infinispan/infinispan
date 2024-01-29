package org.infinispan.server.insights;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.insights.config.InsightsActivation;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.redhat.insights.reports.InsightsReport;

@Test(groups = "functional", testName = "server.insights.InsightsActivationTest")
public class InsightsActivationTest extends AbstractInfinispanTest {

   public void test() throws Exception {
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

      System.setProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME, InsightsActivation.ENABLED.name());
      try (EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createServerModeCacheManager()) {
         InsightsService service = cacheManager.getGlobalComponentRegistry().getComponent(InsightsService.class);
         assertThat(service).isNotNull();
         InsightsReport report = service.report();
         String json = report.serialize();
         assertThat(json).isNotEmpty();
      }
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void testClassFinished(ITestContext context) {
      System.clearProperty(InsightsModule.REDHAT_INSIGHTS_ACTIVATION_PROPERTY_NAME);
      super.testClassFinished(context);
   }
}
