package org.infinispan.server.functional;

import static java.util.Collections.singleton;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @since 10.0
 */
public class IgnoreCaches {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   private final ObjectMapper mapper = new ObjectMapper();

   private static final String CACHE_MANAGER = "default";

   @Test
   public void testIgnoreCaches() throws Exception {
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
      RestResponse restResponse = sync(client.cache(cacheName).get("whatever"));
      assertEquals(code, restResponse.getStatus());
   }

   private void unIgnoreCache(RestClient client, String cacheName) {
      RestResponse response = sync(client.server().unIgnoreCache(CACHE_MANAGER, cacheName));
      assertEquals(204, response.getStatus());
   }

   private void ignoreCache(RestClient client, String cacheName) {
      RestResponse response = sync(client.server().ignoreCache(CACHE_MANAGER, cacheName));
      assertEquals(204, response.getStatus());
   }

   private Set<String> getIgnoredCaches(RestClient client, String cacheManagerName) throws Exception {
      RestResponse response = sync(client.server().listIgnoredCaches(cacheManagerName));
      assertEquals(200, response.getStatus());
      Set<String> res = new HashSet<>();
      mapper.readTree(response.getBody()).elements().forEachRemaining(n -> res.add(n.asText()));
      return res;
   }
}
