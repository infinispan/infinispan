package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.IpFilterRule;
import org.infinispan.client.rest.RestLoggingClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

import okhttp3.FormBody;
import okhttp3.Request;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestServerClientOkHttp implements RestServerClient {
   private final RestClientOkHttp client;
   private final String baseServerURL;

   RestServerClientOkHttp(RestClientOkHttp restClient) {
      this.client = restClient;
      this.baseServerURL = String.format("%s%s/v2/server", restClient.getBaseURL(), restClient.getConfiguration().contextPath()).replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> configuration() {
      return client.execute(baseServerURL, "config");
   }

   @Override
   public CompletionStage<RestResponse> stop() {
      Request.Builder builder = new Request.Builder().post(EMPTY_BODY).url(baseServerURL + "?action=stop");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> threads() {
      return client.execute(baseServerURL, "threads");
   }

   @Override
   public CompletionStage<RestResponse> info() {
      return client.execute(baseServerURL);
   }

   @Override
   public CompletionStage<RestResponse> memory() {
      return client.execute(baseServerURL, "memory");
   }

   @Override
   public CompletionStage<RestResponse> heapDump(boolean live) {
      String url = String.format("%s/memory?action=heap-dump&live=%b", baseServerURL, live);
      return client.execute(new Request.Builder().url(url).method("POST",new FormBody.Builder().build()));
   }

   @Override
   public CompletionStage<RestResponse> env() {
      return client.execute(baseServerURL, "env");
   }

   @Override
   public CompletionStage<RestResponse> report() {
      return client.execute(baseServerURL, "report");
   }

   @Override
   public CompletionStage<RestResponse> ignoreCache(String cacheManagerName, String cacheName) {
      return ignoreCacheOp(cacheManagerName, cacheName, "POST");
   }

   @Override
   public CompletionStage<RestResponse> unIgnoreCache(String cacheManagerName, String cacheName) {
      return ignoreCacheOp(cacheManagerName, cacheName, "DELETE");
   }

   private CompletionStage<RestResponse> ignoreCacheOp(String cacheManagerName, String cacheName, String method) {
      String url = String.format("%s/ignored-caches/%s/%s", baseServerURL, cacheManagerName, cacheName);
      Request.Builder builder = new Request.Builder().url(url).method(method, new FormBody.Builder().build());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> listIgnoredCaches(String cacheManagerName) {
      String url = String.format("%s/ignored-caches/%s", baseServerURL, cacheManagerName);
      return client.execute(new Request.Builder().url(url));
   }

   @Override
   public RestLoggingClient logging() {
      return new RestLoggingClientOkHttp(client);
   }

   @Override
   public CompletionStage<RestResponse> connectorNames() {
      return client.execute(baseServerURL, "connectors");
   }

   @Override
   public CompletionStage<RestResponse> connectorStart(String name) {
      String url = String.format("%s/connectors/%s?action=start", baseServerURL, name);
      Request.Builder builder = new Request.Builder().url(url).post(new FormBody.Builder().build());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> connectorStop(String name) {
      String url = String.format("%s/connectors/%s?action=stop", baseServerURL, name);
      Request.Builder builder = new Request.Builder().url(url).post(new FormBody.Builder().build());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> connector(String name) {
      return client.execute(baseServerURL, "connectors", name);
   }

   @Override
   public CompletionStage<RestResponse> connectorIpFilters(String name) {
      return client.execute(baseServerURL, "connectors", name, "ip-filter");
   }

   @Override
   public CompletionStage<RestResponse> connectorIpFiltersClear(String name) {
      String url = String.format("%s/connectors/%s/ip-filter", baseServerURL, name);
      Request.Builder builder = new Request.Builder().url(url).delete();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> connectorIpFilterSet(String name, List<IpFilterRule> rules) {
      String url = String.format("%s/connectors/%s/ip-filter", baseServerURL, name);
      Json json = Json.array();
      for (IpFilterRule rule : rules) {
         json.add(Json.object().set("type", rule.getType().name()).set("cidr", rule.getCidr()));
      }
      Request.Builder builder = new Request.Builder().url(url).post(new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, json.toString()).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> dataSourceNames() {
      return client.execute(baseServerURL, "datasources");
   }

   @Override
   public CompletionStage<RestResponse> dataSourceTest(String name) {
      String url = String.format("%s/datasources/%s?action=test", baseServerURL, name);
      Request.Builder builder = new Request.Builder().url(url).post(new FormBody.Builder().build());
      return client.execute(builder);
   }
}
