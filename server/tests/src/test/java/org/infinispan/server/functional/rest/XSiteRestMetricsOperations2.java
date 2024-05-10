package org.infinispan.server.functional.rest;

import static org.infinispan.server.functional.rest.RestMetricsResourceIT.getMetrics;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.metrics.Constants;
import org.infinispan.server.functional.XSite2ServersIT;
import org.infinispan.server.test.core.Common;
import org.infinispan.server.test.junit5.InfinispanXSiteServerExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Test site status metrics with multiple servers
 *
 * @since 15.0
 */
public class XSiteRestMetricsOperations2 {

   @RegisterExtension
   public static final InfinispanXSiteServerExtension SERVERS = XSite2ServersIT.SERVERS;


   private static final String FAKE_SITE_CACHE_XML_CONFIG =
               "  <replicated-cache name=\"%s\" statistics=\"true\">" +
               "     <backups>" +
               "        <backup site=\"SFO\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>";

   @Test
   public void testCrossSiteViewStatus() {
      var sfoCacheName = "another-" + SERVERS.getMethodName();

      var lonXML = String.format(XSite2ServersIT.LON_CACHE_CONFIG, SERVERS.getMethodName());
      var nycXML = String.format(XSite2ServersIT.NYC_CACHE_CONFIG, SERVERS.getMethodName());
      var sfoXML = String.format(FAKE_SITE_CACHE_XML_CONFIG, sfoCacheName);

      //noinspection resource
      SERVERS.rest(LON).withServerConfiguration(new StringConfiguration(lonXML)).create();
      //noinspection resource
      SERVERS.rest(NYC).withServerConfiguration(new StringConfiguration(nycXML)).create();
      //noinspection resource
      Common.sync(SERVERS.rest(LON).get().cache(sfoCacheName).createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, sfoXML)));


      var siteMasterMetrics = SERVERS.rest(LON).get(0).metrics();
      var nonSiteMasterMetrics = SERVERS.rest(LON).get(1).metrics();

      assertCrossSiteViewStatus(siteMasterMetrics, Map.of(NYC, 1.0, "SFO", 0.0));
      assertCrossSiteViewStatus(nonSiteMasterMetrics, Map.of(NYC, 2.0, "SFO", 2.0));
   }

   @Test
   public void testCrossSiteViewStatusAfterStop() {
      var sfoCacheName = "another-" + SERVERS.getMethodName();

      var lonXML = String.format(XSite2ServersIT.LON_CACHE_CONFIG, SERVERS.getMethodName());
      var nycXML = String.format(XSite2ServersIT.NYC_CACHE_CONFIG, SERVERS.getMethodName());
      var sfoXML = String.format(FAKE_SITE_CACHE_XML_CONFIG, sfoCacheName);

      //noinspection resource
      SERVERS.rest(LON).withServerConfiguration(new StringConfiguration(lonXML)).create();
      //noinspection resource
      SERVERS.rest(NYC).withServerConfiguration(new StringConfiguration(nycXML)).create();
      //noinspection resource
      Common.sync(SERVERS.rest(LON).get().cache(sfoCacheName).createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, sfoXML)));


      var siteMasterMetrics = SERVERS.rest(LON).get(0).metrics();
      var nonSiteMasterMetrics = SERVERS.rest(LON).get(1).metrics();

      assertCrossSiteViewStatus(siteMasterMetrics, Map.of(NYC, 1.0, "SFO", 0.0));
      assertCrossSiteViewStatus(nonSiteMasterMetrics, Map.of(NYC, 2.0, "SFO", 2.0));

      var testServer = SERVERS.getTestServers().stream().filter(s -> s.getSiteName().equals(NYC)).findAny();
      assertTrue(testServer.isPresent());

      var driver = testServer.get().getDriver();
      IntStream.range(0, XSite2ServersIT.NUM_SERVERS).forEach(driver::stop);
      IntStream.range(0, XSite2ServersIT.NUM_SERVERS).mapToObj(driver::isRunning).forEach(Assertions::assertFalse);

      assertCrossSiteViewStatus(siteMasterMetrics, Map.of(NYC, 0.0, "SFO", 0.0));
      assertCrossSiteViewStatus(nonSiteMasterMetrics, Map.of(NYC, 2.0, "SFO", 2.0));

      driver.restartCluster();
      IntStream.range(0, XSite2ServersIT.NUM_SERVERS).mapToObj(driver::isRunning).forEach(Assertions::assertTrue);

      assertCrossSiteViewStatus(siteMasterMetrics, Map.of(NYC, 1.0, "SFO", 0.0));
      assertCrossSiteViewStatus(nonSiteMasterMetrics, Map.of(NYC, 2.0, "SFO", 2.0));
   }

   private static void assertCrossSiteViewStatus(RestMetricsClient client, Map<String, Double> expectedSites) {
      var metrics = getMetrics(client).stream()
            .filter(metric -> metric.matches("vendor_jgroups_site_view_status"))
            .toList();
      assertEquals(expectedSites.size(), metrics.size(), "Wrong metrics: " + metrics);
      expectedSites.forEach((site, expected) -> {
         var filtered = metrics.stream().filter(metricInfo -> metricInfo.containsTag(Constants.SITE_TAG_NAME, site)).toList();
         assertEquals(1, filtered.size(), "Wrong number of metrics: " + metrics);
         filtered.get(0).value().isEqualTo(expected);
      });

   }


}
