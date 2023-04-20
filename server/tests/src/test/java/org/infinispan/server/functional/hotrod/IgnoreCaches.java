package org.infinispan.server.functional.hotrod;

import static java.util.Collections.singleton;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.rest.helper.RestResponses.assertStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.helper.RestResponses;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.0
 */
public class IgnoreCaches {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private static final String CACHE_MANAGER = "default";

   @Test
   public void testIgnoreCaches() {
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder();
      RestClient client = SERVER_TEST.rest().withClientConfiguration(builder).create();
      String testCache = SERVER_TEST.getMethodName();

      assertTrue(getIgnoredCaches(client, CACHE_MANAGER).isEmpty());
      assertCacheResponse(client, testCache, 404);
      assertCacheResponse(client, PROTOBUF_METADATA_CACHE_NAME, 404);

      ignoreCache(client, testCache);
      assertEquals(singleton(testCache), getIgnoredCaches(client, CACHE_MANAGER));
      assertCacheResponse(client, testCache, 503);
      assertCacheResponse(client, PROTOBUF_METADATA_CACHE_NAME, 404);

      ignoreCache(client, PROTOBUF_METADATA_CACHE_NAME);
      assertEquals(asSet(testCache, PROTOBUF_METADATA_CACHE_NAME), getIgnoredCaches(client, CACHE_MANAGER));
      assertCacheResponse(client, testCache, 503);
      assertCacheResponse(client, PROTOBUF_METADATA_CACHE_NAME, 503);

      unIgnoreCache(client, testCache);
      assertEquals(singleton(PROTOBUF_METADATA_CACHE_NAME), getIgnoredCaches(client, CACHE_MANAGER));
      assertCacheResponse(client, testCache, 404);
      assertCacheResponse(client, PROTOBUF_METADATA_CACHE_NAME, 503);

      unIgnoreCache(client, PROTOBUF_METADATA_CACHE_NAME);
      assertTrue(getIgnoredCaches(client, CACHE_MANAGER).isEmpty());
      assertCacheResponse(client, testCache, 404);
      assertCacheResponse(client, PROTOBUF_METADATA_CACHE_NAME, 404);
   }

   private Set<String> asSet(String... elements) {
      return Arrays.stream(elements).collect(Collectors.toSet());
   }

   private void assertCacheResponse(RestClient client, String cacheName, int code) {
      assertStatus(code, client.cache(cacheName).get("whatever"));
   }

   private void unIgnoreCache(RestClient client, String cacheName) {
      assertStatus(204, client.server().unIgnoreCache(CACHE_MANAGER, cacheName));
   }

   private void ignoreCache(RestClient client, String cacheName) {
      assertStatus(204, client.server().ignoreCache(CACHE_MANAGER, cacheName));
   }

   private Set<String> getIgnoredCaches(RestClient client, String cacheManagerName) {
      Json body = RestResponses.jsonResponseBody(client.server().listIgnoredCaches(cacheManagerName));
      return body.asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
   }
}
