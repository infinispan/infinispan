package org.infinispan.server.test.junit5;

import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.LON;
import static org.infinispan.server.test.core.InfinispanServerTestConfiguration.NYC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.test.Eventually;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Gustavo Lira
 * @since 12
 */
public class InfinispanXSiteServerTest {

   protected static final int NUM_SERVERS = 1;
   protected static final String LON_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <replicated-cache name=\"%s\">" +
               "     <backups>" +
               "        <backup site=\"" + NYC + "\" strategy=\"SYNC\"/>" +
               "     </backups>" +
               "  </replicated-cache>" +
               "</cache-container></infinispan>";

   static final InfinispanServerExtensionBuilder LON_SERVER = InfinispanServerExtensionBuilder.config("XSiteServerTest.xml").numServers(NUM_SERVERS);
   static final InfinispanServerExtensionBuilder NYC_SERVER = InfinispanServerExtensionBuilder.config("XSiteServerTest.xml").numServers(NUM_SERVERS);

   @RegisterExtension
   static InfinispanXSiteServerExtension SERVER_TEST = new InfinispanXSiteServerExtensionBuilder()
         .addSite(LON, LON_SERVER)
         .addSite(NYC, NYC_SERVER)
         .build();

   @Test
   public void testSingleServer() {
      String lonXML = String.format(LON_CACHE_XML_CONFIG, SERVER_TEST.getMethodName());
      RemoteCache<String, String> lonCache = SERVER_TEST.hotrod(LON)
            .withServerConfiguration(new StringConfiguration(lonXML)).create();
      RemoteCache<String, String> nycCache = SERVER_TEST.hotrod(NYC).create(); //nyc cache don't backup to lon

      lonCache.put("k1", "v1");
      nycCache.put("k2", "v2");

      assertEquals("v1", lonCache.get("k1"));
      Eventually.eventuallyEquals("v1", () -> nycCache.get("k1"));
      assertEquals(null, lonCache.get("k2"));
      assertEquals("v2", nycCache.get("k2"));
   }
}
