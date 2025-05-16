package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.RestHeaders.ACCEPT;
import static org.infinispan.client.rest.RestHeaders.ACCEPT_ENCODING;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.IpFilterRule;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestLoggingClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

public class RestServerClientJDK implements RestServerClient {
   private final RestRawClientJDK client;
   private final String path;

   RestServerClientJDK(RestRawClientJDK restClient) {
      this.client = restClient;
      this.path = restClient.getConfiguration().contextPath() + "/v2/server";
   }

   @Override
   public CompletionStage<RestResponse> configuration() {
      return client.get(path + "/config");
   }

   @Override
   public CompletionStage<RestResponse> stop() {
      return client.post(path + "?action=stop");
   }

   @Override
   public CompletionStage<RestResponse> overviewReport() {
      return client.get(path + "/overview-report");
   }

   @Override
   public CompletionStage<RestResponse> threads() {
      return client.get(path + "/threads");
   }

   @Override
   public CompletionStage<RestResponse> info() {
      return client.get(path);
   }

   @Override
   public CompletionStage<RestResponse> memory() {
      return client.get(path + "/memory");
   }

   @Override
   public CompletionStage<RestResponse> heapDump(boolean live) {
      return client.post(path + "/memory?action=heap-dump&live=" + live);
   }

   @Override
   public CompletionStage<RestResponse> env() {
      return client.get(path + "/env");
   }

   @Override
   public CompletionStage<RestResponse> report() {
      return client.get(path + "/report", Map.of(ACCEPT, MediaType.APPLICATION_GZIP_TYPE, ACCEPT_ENCODING, "gzip"));
   }

   @Override
   public CompletionStage<RestResponse> report(String node) {
      return client.get(path + "/report/" + node, Map.of(ACCEPT, MediaType.APPLICATION_GZIP_TYPE, ACCEPT_ENCODING, "gzip"));
   }

   @Override
   public CompletionStage<RestResponse> ignoreCache(String cacheName) {
      return client.post(path + "/ignored-caches/" + cacheName);
   }

   @Override
   public CompletionStage<RestResponse> unIgnoreCache(String cacheName) {
      return client.delete(path + "/ignored-caches/" + cacheName);
   }

   @Override
   public CompletionStage<RestResponse> listIgnoredCaches() {
      return client.get(path + "/ignored-caches");
   }

   @Override
   public RestLoggingClient logging() {
      return new RestLoggingClientJDK(client);
   }

   @Override
   public CompletionStage<RestResponse> listConnections(boolean global) {
      return client.get(path + "/connections?global=" + global);
   }

   @Override
   public CompletionStage<RestResponse> connectorNames() {
      return client.get(path + "/connectors");
   }

   @Override
   public CompletionStage<RestResponse> connectorStart(String name) {
      return client.post(path + "/connectors/" + name + "?action=start");
   }

   @Override
   public CompletionStage<RestResponse> connectorStop(String name) {
      return client.post(path + "/connectors/" + name + "?action=stop");
   }

   @Override
   public CompletionStage<RestResponse> connector(String name) {
      return client.get(path + "/connectors/" + name);
   }

   @Override
   public CompletionStage<RestResponse> connectorIpFilters(String name) {
      return client.get(path + "/connectors/" + name + "/ip-filter");
   }

   @Override
   public CompletionStage<RestResponse> connectorIpFiltersClear(String name) {
      return client.delete(path + "/connectors/" + name + "/ip-filter");
   }

   @Override
   public CompletionStage<RestResponse> connectorIpFilterSet(String name, List<IpFilterRule> rules) {
      String url = String.format("%s/connectors/%s/ip-filter", path, name);
      Json json = Json.array();
      for (IpFilterRule rule : rules) {
         json.add(Json.object().set("type", rule.getType().name()).set("cidr", rule.getCidr()));
      }
      return client.post(url, RestEntity.create(MediaType.APPLICATION_JSON, json.toString()));
   }

   @Override
   public CompletionStage<RestResponse> dataSourceNames() {
      return client.get(path + "/datasources");
   }

   @Override
   public CompletionStage<RestResponse> dataSourceTest(String name) {
      return client.post(path + "/datasources/" + name + "?action=test");
   }

   @Override
   public CompletionStage<RestResponse> cacheConfigDefaults() {
      return client.get(path + "/caches/defaults");
   }

   @Override
   public CompletionStage<RestResponse> ready() {
      return client.head("/health/ready");
   }

   @Override
   public CompletionStage<RestResponse> live() {
      return client.head("/health/live");
   }
}
