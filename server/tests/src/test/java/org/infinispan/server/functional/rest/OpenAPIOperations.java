package org.infinispan.server.functional.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.client.openapi.ApiException;
import org.infinispan.client.openapi.OpenAPIClient;
import org.infinispan.client.openapi.api.CacheApi;
import org.infinispan.client.openapi.configuration.OpenAPIClientConfigurationBuilder;
import org.infinispan.client.openapi.configuration.Protocol;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * @since 16.0
 **/
public class OpenAPIOperations {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @ParameterizedTest
   @EnumSource(Protocol.class)
   public void testRestOperations(Protocol protocol) throws ApiException {
      OpenAPIClientConfigurationBuilder builder = new OpenAPIClientConfigurationBuilder();
      builder.protocol(protocol);
      OpenAPIClient client = SERVERS.openapi().withClientConfiguration(builder).create();
      CacheApi cache = client.cache();
      String cacheName = SERVERS.getMethodName();
      cache.putCacheEntry(cacheName, "k1", "v1", MediaType.TEXT_PLAIN_TYPE, null, null);
      String v = cache.getCacheEntry(cacheName, "k1", null);
      assertEquals("v1", v);
   }
}
