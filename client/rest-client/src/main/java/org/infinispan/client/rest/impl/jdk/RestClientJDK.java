package org.infinispan.client.rest.impl.jdk;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestCounterClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;
import org.infinispan.client.rest.RestSecurityClient;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.client.rest.RestTaskClient;
import org.infinispan.client.rest.configuration.RestClientConfiguration;

/**
 * @since 15.0
 **/
public class RestClientJDK implements RestClient {
   public static final String CONTENT_TYPE = "Content-Type";
   public static final String CONTENT_DISPOSITION = "Content-Disposition";
   public static final String CONTENT_LENGTH = "Content-Length";
   public static final String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
   public static final String ACCEPT = "Accept";
   public static final String AUTHORIZATION = "Authorization";

   private final RestRawClientJDK rawClient;
   private final String contextPath;

   public RestClientJDK(RestClientConfiguration configuration) {
      this.contextPath = configuration.contextPath();
      this.rawClient = new RestRawClientJDK(configuration);
   }

   @Override
   public void close() throws Exception {
      rawClient.close();
   }

   @Override
   public RestServerClient server() {
      return new RestServerClientJDK(rawClient);
   }

   @Override
   public RestClusterClient cluster() {
      return new RestClusterClientJDK(rawClient);
   }

   @Override
   public CompletionStage<RestResponse> cacheManagers() {
      return rawClient.get(contextPath + "/v2/server/cache-managers");
   }

   @Override
   public RestContainerClient container() {
      return new RestContainerClientJDK(rawClient);
   }

   @Override
   public CompletionStage<RestResponse> caches() {
      return rawClient.get(contextPath + "/v2/caches");
   }

   @Override
   public CompletionStage<RestResponse> detailedCacheList() {
      return rawClient.get(contextPath + "/v2/caches?action=detailed");
   }

   @Override
   public CompletionStage<RestResponse> cachesByRole(String roleName) {
      return rawClient.get(contextPath + "/v2/caches?action=role-accessible&role=" + roleName);
   }

   @Override
   public RestCacheClient cache(String name) {
      return new RestCacheClientJDK(rawClient, name);
   }

   @Override
   public CompletionStage<RestResponse> counters() {
      return rawClient.get(contextPath + "/v2/counters");
   }

   @Override
   public RestCounterClient counter(String name) {
      return new RestCounterClientJDK(rawClient, name);
   }

   @Override
   public RestTaskClient tasks() {
      return new RestTaskClientJDK(rawClient);
   }

   @Override
   public RestRawClient raw() {
      return rawClient;
   }

   static String sanitize(String s) {
      return URLEncoder.encode(s, StandardCharsets.UTF_8);
   }

   @Override
   public RestMetricsClient metrics() {
      return new RestMetricsClientJDK(rawClient);
   }

   @Override
   public RestSchemaClient schemas() {
      return new RestSchemaClientJDK(rawClient);
   }

   @Override
   public RestClientConfiguration getConfiguration() {
      return rawClient.getConfiguration();
   }

   @Override
   public RestSecurityClient security() {
      return new RestSecurityClientJDK(rawClient);
   }
}
