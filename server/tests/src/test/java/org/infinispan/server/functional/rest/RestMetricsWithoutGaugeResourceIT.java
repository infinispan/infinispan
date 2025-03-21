package org.infinispan.server.functional.rest;

import static org.infinispan.server.functional.rest.RestMetricsResourceIT.findMetric;
import static org.infinispan.server.functional.rest.RestMetricsResourceIT.getMetrics;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RestMetricsWithoutGaugeResourceIT {

   private static final int NUM_SERVERS = 2;

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerWithoutGaugeTest.xml")
               .numServers(NUM_SERVERS)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testJvmMetrics() {
      var metrics = getMetrics(SERVERS.rest().create().metrics());
      findMetric(metrics, "jvm_classes_loaded_classes").value().isPositive();
      findMetric(metrics, "jvm_memory_used_bytes").value().isPositive();
   }

   @Test
   public void testGaugeMissing() {
      var metrics = getMetrics(SERVERS.rest().create().metrics());
      assertTrue(metrics.stream()
            .noneMatch(metric -> metric.matches("jgroups_stats_async_requests_total")));
   }

}
