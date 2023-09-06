package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.functional.XSiteIT;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = XSiteIT.SERVERS;

   // copied from regex101.com
   private static final Pattern PROMETHEUS_PATTERN = Pattern.compile("^(?<metric>[a-zA-Z_:][a-zA-Z0-9_:]*]*)(?<tags>\\{.*})?[\\t ]*(?<value>-?[0-9E.]*)[\\t ]*(?<timestamp>[0-9]+)?$");
   private static final String[] TAGGED_METRICS = {
         // TakeOfflineManager metrics
         "x_site_admin_status",
         "x_site_admin_millis_since_first_failure",
         "x_site_admin_failures_count",

         // RpcManager metrics/gauges
         "rpc_manager_number_xsite_requests_received_from_site",
         "rpc_manager_number_xsite_requests_sent_to_site",
         "rpc_manager_minimum_xsite_replication_time_to_site",
         "rpc_manager_average_xsite_replication_time_to_site",
         "rpc_manager_maximum_xsite_replication_time_to_site",

         // RpcManager metrics/histogram
         "rpc_manager_replication_times_to_site_seconds_count",
         "rpc_manager_replication_times_to_site_seconds_sum",
         "rpc_manager_replication_times_to_site_seconds_max",
   };
   private static final String[] UNTAGGED_METRICS = {
         // RpcManager metrics/gauges
         "rpc_manager_number_xsite_requests_received",
         "rpc_manager_number_xsite_requests",
         "rpc_manager_minimum_xsite_replication_time",
         "rpc_manager_average_xsite_replication_time",
         "rpc_manager_maximum_xsite_replication_time",

         // RpcManager metrics/histogram
         "rpc_manager_cross_site_replication_times_seconds_count",
         "rpc_manager_cross_site_replication_times_seconds_sum",
         "rpc_manager_cross_site_replication_times_seconds_max",
   };

   @Test
   public void testSiteStatus() throws Exception {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, SERVERS.getMethodName());
      String nycXML = String.format(NYC_CACHE_XML_CONFIG, SERVERS.getMethodName());

      //noinspection resource
      RestClient client = SERVERS.rest(LON).withServerConfiguration(new StringConfiguration(lonXML)).create();
      RestMetricsClient metricsClient = client.metrics();

      // create cache in NYC
      //noinspection resource
      SERVERS.rest(NYC).withServerConfiguration(new StringConfiguration(nycXML)).create();

      String statusMetricName = "x_site_admin_status";

      try (RestResponse response = sync(metricsClient.metrics(true))) {
         assertEquals(200, response.getStatus());
         RestMetricsResource.checkIsOpenmetrics(response.contentType());
         assertTrue(response.getBody().contains("# TYPE vendor_" + statusMetricName + " gauge\n"));
      }

      assertSiteStatusMetrics(metricsClient, statusMetricName, 1);

      try (RestResponse response = sync(client.cacheManager("default").takeOffline(NYC))) {
         assertEquals(200, response.getStatus());
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

      Map<String, String> metricsAndTags = getMetrics(metricsClient);

      for (String metric : TAGGED_METRICS) {
         expectNycSiteTag(true, metric, metricsAndTags);
      }

      for (String metric : UNTAGGED_METRICS) {
         expectNycSiteTag(false, metric, metricsAndTags);
      }
   }

   private static Map<String, String> getMetrics(RestMetricsClient client) {
      Map<String, String> metricsToTags = new HashMap<>();
      try (RestResponse response = sync(client.metrics(true))) {
         assertEquals(200, response.getStatus());
         RestMetricsResource.checkIsOpenmetrics(response.contentType());
         for (Iterator<String> it = response.getBody().lines().iterator() ; it.hasNext() ; ) {
            Matcher matcher = PROMETHEUS_PATTERN.matcher(it.next());
            if (matcher.matches()) {
               metricsToTags.put(matcher.group("metric"), matcher.group("tags"));
            }
         }
      }
      return metricsToTags;
   }

   private void expectNycSiteTag(boolean containsTag, String metric, Map<String, String> metricsAndTags) {
      String tags = metricsAndTags.get("vendor_" + metric);
      assertNotNull(tags, "Missing tag. metric: " + metric);
      assertEquals(containsTag, tags.contains("site=\"NYC\""), "Unexpected tag. metric: " + metric + ", tags=" + tags);
   }

   private static void assertSiteStatusMetrics(RestMetricsClient client, String metric, int expected) throws Exception {
      try (RestResponse response = sync(client.metrics())) {
         assertEquals(OK, response.getStatus());
         RestMetricsResource.checkIsPrometheus(response.contentType());
         RestMetricsResource.checkRule(response.getBody(), "vendor_" + metric, (stringValue) -> {
            int parsed = (int) Double.parseDouble(stringValue);
            assertThat(parsed).isEqualTo(expected);
         });
      }
   }

}
