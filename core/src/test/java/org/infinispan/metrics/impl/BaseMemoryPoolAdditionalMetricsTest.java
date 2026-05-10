package org.infinispan.metrics.impl;

import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * @author Alexander Schwartz
 */
@Deprecated(forRemoval = true, since = "15.2")
@Test(groups = "functional", testName = "metrics.BaseMemoryPoolAdditionalMetricsTest")
public class BaseMemoryPoolAdditionalMetricsTest {

   @Test
   public void testMetricNamesContainOnlyValidCharacters() {
      SimpleMeterRegistry registry = new SimpleMeterRegistry();
      new BaseMemoryPoolAdditionalMetrics().bindTo(registry);
      SoftAssertions asserts = new SoftAssertions();
      registry.getMeters().forEach(meter -> {
         asserts.assertThat(meter.getId().getName()).matches("[.\\w]+").as("metric name contains invalid characters: %s", meter.getId().getName());
         asserts.assertThat(meter.getId().getName()).matches("^[^_].*[^_]$").as("metric name should not begin or end with an underscore: %s", meter.getId().getName());
      });
      asserts.assertAll();
      registry.close();
   }

}
