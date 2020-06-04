package org.infinispan.server.functional;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Eventually;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.infinispan.util.concurrent.CompletionStages;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @since 11.0
 **/
public class XSiteRestCacheOperations {

   @ClassRule
   public static final InfinispanServerRule LON_SERVERS = XSiteIT.LON_SERVERS;
   @ClassRule
   public static final InfinispanServerRule NYC_SERVERS = XSiteIT.NYC_SERVERS;

   private static final String LON_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"NYC\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";
   @Rule
   public InfinispanServerTestMethodRule LON_SERVER_TEST = new InfinispanServerTestMethodRule(XSiteIT.LON_SERVERS);

   @Rule
   public InfinispanServerTestMethodRule NYC_SERVER_TEST = new InfinispanServerTestMethodRule(XSiteIT.NYC_SERVERS);

   private static void assertStatus(int status, CompletionStage<RestResponse> stage) {
      Assert.assertEquals(status, CompletionStages.join(stage).getStatus());
   }

   private static String bodyOf(CompletionStage<RestResponse> stage) {
      RestResponse rsp = CompletionStages.join(stage);
      return rsp.getStatus() == 200 ? rsp.getBody() : null;
   }

   @Test
   public void testRestOperations() {
      String cacheName = LON_SERVER_TEST.getMethodName();
      String lonXML = String.format(LON_CACHE_XML_CONFIG, cacheName);

      RestClient lonClient = LON_SERVER_TEST.rest().get();
      RestClient nycClient = NYC_SERVER_TEST.rest().get();

      RestCacheClient lonCache = lonClient.cache(cacheName);
      RestCacheClient nycCache = nycClient.cache(cacheName);
      assertStatus(200, lonCache.createWithConfiguration(RestEntity.create(MediaType.APPLICATION_XML, lonXML)));
      assertStatus(200, nycCache.createWithTemplate(DefaultTemplate.DIST_SYNC.getTemplateName()));


      assertStatus(204, lonCache.put("k1", "v1"));
      assertStatus(204, nycCache.put("k2", "v2")); //nyc cache don't backup to lon

      Eventually.eventuallyEquals("v1", () -> bodyOf(lonCache.get("k1")));
      Eventually.eventuallyEquals("v1", () -> bodyOf(nycCache.get("k1")));

      Eventually.eventuallyEquals(null, () -> bodyOf(lonCache.get("k2")));
      Eventually.eventuallyEquals("v2", () -> bodyOf(nycCache.get("k2")));
   }
}
