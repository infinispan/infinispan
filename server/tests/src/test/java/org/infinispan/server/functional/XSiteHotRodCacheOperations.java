package org.infinispan.server.functional;

import static org.infinispan.commons.test.Eventually.eventuallyEquals;
import static org.infinispan.server.functional.XSiteIT.LON;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.functional.XSiteIT.LON_CACHE_OFF_HEAP;
import static org.infinispan.server.functional.XSiteIT.NYC;
import static org.infinispan.server.functional.XSiteIT.NYC_CACHE_CUSTOM_NAME_XML_CONFIG;
import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit4.InfinispanXSiteServerRule;
import org.infinispan.server.test.junit4.InfinispanXSiteServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Pedro Ruivo
 * @author Gustavo Lira
 * @since 11.0
 **/
public class XSiteHotRodCacheOperations {

   @ClassRule
   public static final InfinispanXSiteServerRule SERVERS = XSiteIT.SERVERS;

   @Rule
   public InfinispanXSiteServerTestMethodRule SERVER_TEST = new InfinispanXSiteServerTestMethodRule(SERVERS);

   @Test
   public void testHotRodOperations() {
      String lonXML = String.format(XSiteIT.LON_CACHE_XML_CONFIG, SERVER_TEST.getMethodName());
      RemoteCache<String, String> lonCache = SERVER_TEST.hotrod(LON)
            .withServerConfiguration(new XMLStringConfiguration(lonXML)).create();
      RemoteCache<String, String> nycCache = SERVER_TEST.hotrod(NYC).create(); //nyc cache don't backup to lon

      insertAndVerifyEntries(lonCache, nycCache, false);
   }

   @Test
   public void testHotRodOperationsWithDifferentCacheName() {
      RemoteCache<String, String> lonCache = SERVER_TEST.hotrod(LON)
            .createRemoteCacheManager()
            .administration()
            .createCache("lon-cache", new XMLStringConfiguration(LON_CACHE_CUSTOM_NAME_XML_CONFIG));

      RemoteCache<String, String> nycCache = SERVER_TEST.hotrod(NYC)
            .createRemoteCacheManager()
            .administration()
            .createCache("nyc-cache", new XMLStringConfiguration(NYC_CACHE_CUSTOM_NAME_XML_CONFIG));

      insertAndVerifyEntries(lonCache, nycCache, true);
   }

   @Test
   public void testHotRodOperationsWithOffHeapSingleFileStore() {
      String lonXML = String.format(LON_CACHE_OFF_HEAP, SERVER_TEST.getMethodName());
      RemoteCache<Integer, Integer> lonCache = SERVER_TEST.hotrod(LON)
            .withServerConfiguration(new XMLStringConfiguration(lonXML)).create();
      RemoteCache<Integer, Integer> nycCache = SERVER_TEST.hotrod(NYC).create(); //nyc cache don't backup to lon

      //Just to make sure that the file store is empty
      assertEquals(0, getTotalMemoryEntries(lonXML));

      IntStream.range(0, 300).forEach(i -> lonCache.put(i, i));

      eventuallyEquals(300, () -> nycCache.size());
      assertEquals(100, getTotalMemoryEntries(lonXML));
   }

   private int getTotalMemoryEntries(String lonXML) {
      RestClient restClient = SERVER_TEST.rest(LON)
            .withServerConfiguration(new XMLStringConfiguration(lonXML)).get();

      RestCacheClient client = restClient.cache(SERVER_TEST.getMethodName());
      Json json = Json.read(sync(client.stats()).getBody());
      return json.asJsonMap().get("current_number_of_entries_in_memory").asInteger();
   }

   private void insertAndVerifyEntries(RemoteCache<String, String> lonCache, RemoteCache<String, String> nycCache, boolean allSitesBackup) {
      lonCache.put("k1", "v1");
      nycCache.put("k2", "v2");

      assertEquals("v1", lonCache.get("k1"));
      eventuallyEquals("v1", () -> nycCache.get("k1"));
      if(allSitesBackup) {
         eventuallyEquals("v2", () -> lonCache.get("k2"));
      } else {
         assertEquals(null, lonCache.get("k2"));
      }
      assertEquals ("v2", nycCache.get("k2"));
   }
}
