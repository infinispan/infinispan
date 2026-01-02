package org.infinispan.metrics.impl;

import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * @author Alexander Schwartz
 */
@Deprecated(forRemoval=true, since = "15.2")
@Test(groups = "functional", testName = "metrics.BaseMemoryPoolAdditionalMetricsTest")
public class BaseMemoryPoolAdditionalMetricsTest {

    @Test
    public void testMetricNamesContainOnlyValidCharacters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new BaseMemoryPoolAdditionalMetrics().bindTo(registry);
        SoftAssert asserts = new SoftAssert();
        registry.getMeters().forEach(meter -> {
            asserts.assertTrue(meter.getId().getName().matches("[.\\w]+"), "metric name contains invalid characters: " + meter.getId().getName());
            asserts.assertTrue(meter.getId().getName().matches("^[^_].*[^_]$"), "metric name should not begin or end with an underscore: " + meter.getId().getName());
        });
        asserts.assertAll();
        registry.close();
    }

}
