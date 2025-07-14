package org.infinispan.server.functional.rest;

import static org.infinispan.server.functional.rest.RestMetricsResourceIT.Metric;
import static org.infinispan.server.functional.rest.RestMetricsResourceIT.getMetrics;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.api.TestClientXSiteDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.api.Test;

/**
 * Test site status metrics
 *
 * @since 14.0
 */
public class XSiteRestMetricsOperations {

   private static final String LON_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"%s\" statistics=\"true\">" +
               "     <backups>" +
               "        <backup site=\"" + NYC + "\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   private static final String NYC_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"%s\" statistics=\"true\">" +
               "     <backups>" +
               "        <backup site=\"" + LON + "\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   @InfinispanServer(XSiteIT.class)
   public static TestClientXSiteDriver SERVERS;


   private static final String[] TAGGED_METRICS = {
         // TakeOfflineManager metrics
         "infinispan_x_site_admin_status",
         "infinispan_x_site_admin_failures_count",
         "infinispan_x_site_admin_millis_since_first_failure",

         // RpcManager metrics/gauges
         "infinispan_rpc_manager_number_xsite_requests_sent_to_site",
         "infinispan_rpc_manager_number_xsite_requests_received_from_site",
         "infinispan_rpc_manager_average_xsite_replication_time_to_site",
         "infinispan_rpc_manager_minimum_xsite_replication_time_to_site",
         "infinispan_rpc_manager_maximum_xsite_replication_time_to_site",

         // RpcManager metrics/histogram
         "infinispan_rpc_manager_replication_times_to_site_seconds_count",
         "infinispan_rpc_manager_replication_times_to_site_seconds_sum",
         "infinispan_rpc_manager_replication_times_to_site_seconds_max",
     };
     private static final String[] UNTAGGED_METRICS = {
         // RpcManager metrics/gauges
         "infinispan_rpc_manager_number_xsite_requests_received",
         "infinispan_rpc_manager_number_xsite_requests",
         "infinispan_rpc_manager_average_xsite_replication_time",
         "infinispan_rpc_manager_minimum_xsite_replication_time",
         "infinispan_rpc_manager_maximum_xsite_replication_time",

         // RpcManager metrics/histogram
         "infinispan_rpc_manager_cross_site_replication_times_seconds_count",
         "infinispan_rpc_manager_cross_site_replication_times_seconds_sum",
         "infinispan_rpc_manager_cross_site_replication_times_seconds_max",
     };

   @Test
   public void testSiteStatus() {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, SERVERS.getMethodName());
      String nycXML = String.format(NYC_CACHE_XML_CONFIG, SERVERS.getMethodName());

      //noinspection resource
      RestClient client = SERVERS.rest(LON).withServerConfiguration(new StringConfiguration(lonXML)).create();
      RestMetricsClient metricsClient = client.metrics();

      // create cache in NYC
      //noinspection resource
      SERVERS.rest(NYC).withServerConfiguration(new StringConfiguration(nycXML)).create();

      String statusMetricName = "infinispan_x_site_admin_status";

      assertTrue(getMetrics(metricsClient).stream().anyMatch(m -> m.matches(statusMetricName)));

      assertSiteStatusMetrics(metricsClient, statusMetricName, 1);

      try (RestResponse response = sync(client.container().takeOffline(NYC))) {
         assertEquals(200, response.status());
      }

      assertSiteStatusMetrics(metricsClient, statusMetricName, 0);
   }

   @Test
   public void testMetricExists() {
      // check uniqueness
      var uniqueMetrics = Stream.concat(Arrays.stream(TAGGED_METRICS), Arrays.stream(UNTAGGED_METRICS))
            .distinct()
            .count();
      assertEquals(TAGGED_METRICS.length + UNTAGGED_METRICS.length, uniqueMetrics, "Metrics names are not unique");


      String lonXML = String.format(LON_CACHE_XML_CONFIG, SERVERS.getMethodName());
      String nycXML = String.format(NYC_CACHE_XML_CONFIG, SERVERS.getMethodName());

      //noinspection resource
      RestClient client = SERVERS.rest(LON).withServerConfiguration(new StringConfiguration(lonXML)).create();
      RestMetricsClient metricsClient = client.metrics();

      // create cache in NYC
      //noinspection resource
      SERVERS.rest(NYC).withServerConfiguration(new StringConfiguration(nycXML)).create();

      List<Metric> metrics = getMetrics(metricsClient);

      for (String metric : TAGGED_METRICS) {
         expectNycSiteTag(true, metric, metrics);
      }

      for (String metric : UNTAGGED_METRICS) {
         expectNycSiteTag(false, metric, metrics);
      }
   }

   private static void expectNycSiteTag(boolean containsTag, String name, List<Metric> metrics) {
      Optional<Metric> optMetric = metrics.stream().filter(metric -> metric.matches(name)).findAny();
      assertTrue(optMetric.isPresent(), "Missing metric: " + name);
      if (containsTag) {
         optMetric.get().assertTagPresent("site", "NYC");
      } else {
         optMetric.get().assertTagMissing("site", "NYC");
      }
   }

   private static void assertSiteStatusMetrics(RestMetricsClient client, String metric, int expected) {
      var m = getMetrics(client).stream().filter(m1 -> m1.matches(metric)).findFirst();
      assertTrue(m.isPresent());
      m.get().value().isEqualTo(expected);
   }


}
