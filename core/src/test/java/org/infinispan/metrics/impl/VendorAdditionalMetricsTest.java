package org.infinispan.metrics.impl;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

/**
 * @author Alexander Schwartz
 */
@Test(groups = "functional", testName = "metrics.VendorAdditionalMetricsTest")
public class VendorAdditionalMetricsTest {

    @Test
    public void testMetricNamesContainOnlyValidCharacters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new VendorAdditionalMetrics().bindTo(registry);
        SoftAssert asserts = new SoftAssert();
        registry.getMeters().forEach(meter -> {
            asserts.assertTrue(meter.getId().getName().matches("[.\\w]+"), "metric name contains invalid characters: " + meter.getId().getName());
            asserts.assertTrue(meter.getId().getName().matches("^[^_].*[^_]$"), "metric name should not begin or end with an underscore: " + meter.getId().getName());
        });
        asserts.assertAll();
        registry.close();
    }

}
