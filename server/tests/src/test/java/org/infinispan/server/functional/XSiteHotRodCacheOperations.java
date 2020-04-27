package org.infinispan.server.functional;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.test.Eventually;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @since 11.0
 **/
public class XSiteHotRodCacheOperations {

   @ClassRule
   public static final InfinispanServerRule LON_SERVERS = XSiteIT.LON_SERVERS;
   @ClassRule
   public static final InfinispanServerRule NYC_SERVERS = XSiteIT.NYC_SERVERS;

   private static final String LON_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <distributed-cache-configuration name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"NYC\" strategy=\"ASYNC\"/>" +
               "     </backups>" +
               "  </distributed-cache-configuration>" +
               "</cache-container></infinispan>";
   @Rule
   public InfinispanServerTestMethodRule LON_SERVER_TEST = new InfinispanServerTestMethodRule(XSiteIT.LON_SERVERS);

   @Rule
   public InfinispanServerTestMethodRule NYC_SERVER_TEST = new InfinispanServerTestMethodRule(XSiteIT.NYC_SERVERS);

   @Test
   public void testHotRodOperations() {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, LON_SERVER_TEST.getMethodName());

      RemoteCache<String, String> lonCache = LON_SERVER_TEST.hotrod()
            .withServerConfiguration(new XMLStringConfiguration(lonXML)).create();
      RemoteCache<String, String> nycCache = NYC_SERVER_TEST.hotrod().create(); //must have the same name as LON cache

      lonCache.put("k1", "v1");
      nycCache.put("k2", "v2"); //nyc cache don't backup to lon

      Eventually.eventuallyEquals("v1", () -> lonCache.get("k1"));
      Eventually.eventuallyEquals("v1", () -> nycCache.get("k1"));

      Eventually.eventuallyEquals(null, () -> lonCache.get("k2"));
      Eventually.eventuallyEquals("v2", () -> nycCache.get("k2"));
   }
}
