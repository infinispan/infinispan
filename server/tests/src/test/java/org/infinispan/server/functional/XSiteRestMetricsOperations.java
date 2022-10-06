package org.infinispan.server.functional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

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

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = XSiteIT.SERVERS;

   @Rule
   public InfinispanXSiteServerTestMethodRule SERVER_TEST = new InfinispanXSiteServerTestMethodRule(SERVERS);

   @Test
   public void testSiteStatus() throws Exception {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, SERVER_TEST.getMethodName());
      String nycXML = String.format(NYC_CACHE_XML_CONFIG, SERVER_TEST.getMethodName());

      RestClient client = SERVER_TEST.rest(LON).withServerConfiguration(new StringConfiguration(lonXML)).create();
      RestMetricsClient metricsClient = client.metrics();

      // create cache in NYC
      SERVER_TEST.rest(NYC).withServerConfiguration(new StringConfiguration(nycXML)).create();

      String statusMetricName = "cache_manager_default_cache_" + SERVER_TEST.getMethodName() + "_x_site_admin_nyc_status";

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
