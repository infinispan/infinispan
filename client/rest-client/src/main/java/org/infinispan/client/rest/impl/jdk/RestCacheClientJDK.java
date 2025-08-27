package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.impl.jdk.Util.sanitize;

import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestHeaders;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.XSiteStateTransferMode;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @since 14.0
 **/
public class RestCacheClientJDK implements RestCacheClient {

   private final RestRawClientJDK client;
   private final String name;
   private final String path;
   private final String rollingUpgradeUrl;

   RestCacheClientJDK(RestRawClientJDK client, String name) {
      this.client = client;
      this.name = name;
      this.path = client.getConfiguration().contextPath() + "/v2/caches/" + sanitize(name);
      this.rollingUpgradeUrl = path + "/rolling-upgrade";
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<RestResponse> health() {
      return client.get(path + "?action=health");
   }

   @Override
   public CompletionStage<RestResponse> clear() {
      return client.post(path + "?action=clear");
   }


   @Override
   public CompletionStage<RestResponse> exists() {
      return client.head(path);
   }

   @Override
   public CompletionStage<RestResponse> synchronizeData(Integer readBatch, Integer threads) {
      StringBuilder sb = new StringBuilder(path + "?action=sync-data");
      if (readBatch != null) {
         sb.append("&read-batch=").append(readBatch);
      }
      if (threads != null) {
         sb.append("&threads=").append(threads);
      }
      return client.post(sb.toString());
   }

   @Override
   public CompletionStage<RestResponse> synchronizeData() {
      return synchronizeData(null, null);
   }

   @Override
   public CompletionStage<RestResponse> disconnectSource() {
      return client.delete(rollingUpgradeUrl + "/source-connection");
   }

   @Override
   public CompletionStage<RestResponse> connectSource(RestEntity value) {
      return client.post(rollingUpgradeUrl + "/source-connection", value);
   }

   @Override
   public CompletionStage<RestResponse> sourceConnected() {
      return client.head(rollingUpgradeUrl + "/source-connection");
   }

   @Override
   public CompletionStage<RestResponse> sourceConnection() {
      return client.get(rollingUpgradeUrl + "/source-connection");
   }

   @Override
   public CompletionStage<RestResponse> size() {
      return client.get(path + "?action=size");
   }

   @Override
   public CompletionStage<RestResponse> post(String key, String value) {
      return client.post(path + "/" + sanitize(key), RestEntity.create(MediaType.TEXT_PLAIN, value));
   }

   @Override
   public CompletionStage<RestResponse> post(String key, RestEntity value) {
      return client.post(path + "/" + sanitize(key), value);
   }

   @Override
   public CompletionStage<RestResponse> post(String key, String value, long ttl, long maxIdle) {
      return client.post(path + "/" + sanitize(key), expiration(ttl, maxIdle), RestEntity.create(MediaType.TEXT_PLAIN, value));
   }

   @Override
   public CompletionStage<RestResponse> post(String key, RestEntity value, long ttl, long maxIdle) {
      return client.post(path + "/" + sanitize(key), expiration(ttl, maxIdle), value);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String value) {
      return client.put(path + "/" + sanitize(key), RestEntity.create(MediaType.TEXT_PLAIN, value));
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value) {
      return client.put(path + "/" + sanitize(key), keyContentType != null ? Map.of("Key-Content-Type", keyContentType) : Collections.emptyMap(), value);

   }

   @Override
   public CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value, Map<String, String> headers) {
      if (keyContentType != null) {
         headers.put("Key-Content-Type", keyContentType);
      }
      return client.put(path + "/" + sanitize(key), headers, value);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value, long ttl, long maxIdle) {
      Map<String, String> headers = expiration(ttl, maxIdle);
      if (keyContentType != null) {
         headers.put("Key-Content-Type", keyContentType);
      }
      return client.put(path + "/" + sanitize(key), headers, value);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, RestEntity value, String... flags) {
      return client.put(path + "/" + sanitize(key), flags.length > 0 ? Map.of("flags", String.join(",", flags)) : Collections.emptyMap(), value);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, RestEntity value) {
      return client.put(path + "/" + sanitize(key), value);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String value, long ttl, long maxIdle) {
      return client.post(path + "/" + sanitize(key), expiration(ttl, maxIdle), RestEntity.create(MediaType.TEXT_PLAIN, value));
   }

   @Override
   public CompletionStage<RestResponse> put(String key, RestEntity value, long ttl, long maxIdle) {
      return client.put(path + "/" + sanitize(key), expiration(ttl, maxIdle), value);
   }

   @Override
   public CompletionStage<RestResponse> get(String key) {
      return client.get(path + "/" + sanitize(key));

   }

   @Override
   public CompletionStage<RestResponse> get(String key, Map<String, String> headers) {
      return client.get(path + "/" + sanitize(key), headers);
   }

   @Override
   public CompletionStage<RestResponse> get(String key, String mediaType) {
      return get(key, mediaType, false);
   }

   @Override
   public CompletionStage<RestResponse> get(String key, String mediaType, boolean extended) {
      String url = path + "/" + sanitize(key);
      if (extended) {
         url = url + "?extended=true";
      }
      return client.get(url, mediaType != null ? Map.of(RestHeaders.ACCEPT, mediaType) : Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> head(String key) {
      return head(key, Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> head(String key, Map<String, String> headers) {
      return client.head(path + "/" + sanitize(key) + "?extended", headers);
   }

   @Override
   public CompletionStage<RestResponse> remove(String key) {
      return remove(key, Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> remove(String key, Map<String, String> headers) {
      return client.delete(path + "/" + sanitize(key) + "?extended", headers);
   }

   @Override
   public CompletionStage<RestResponse> createWithTemplate(String template, CacheContainerAdmin.AdminFlag... flags) {
      return client.post(path + "?template=" + template, enums("flags", flags));
   }

   @Override
   public CompletionStage<RestResponse> createWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags) {
      return client.post(path, enums("flags", flags), configuration);
   }

   @Override
   public CompletionStage<RestResponse> delete() {
      return client.delete(path);
   }

   @Override
   public CompletionStage<RestResponse> keys() {
      return client.get(path + "?action=keys", Collections.emptyMap(), HttpResponse.BodyHandlers::ofInputStream);
   }

   @Override
   public CompletionStage<RestResponse> keys(int limit) {
      return client.get(path + "?action=keys&limit=" + limit, Collections.emptyMap(), HttpResponse.BodyHandlers::ofInputStream);
   }

   @Override
   public CompletionStage<RestResponse> entries() {
      return client.get(path + "?action=entries", Collections.emptyMap(), HttpResponse.BodyHandlers::ofInputStream);
   }

   @Override
   public CompletionStage<RestResponse> entries(boolean contentNegotiation) {
      return client.get(path + "?action=entries&content-negotiation=" + contentNegotiation, Collections.emptyMap(), HttpResponse.BodyHandlers::ofInputStream);
   }

   @Override
   public CompletionStage<RestResponse> entries(int limit) {
      return client.get(path + "?action=entries&limit=" + limit, Collections.emptyMap(), HttpResponse.BodyHandlers::ofInputStream);
   }

   @Override
   public CompletionStage<RestResponse> entries(int limit, boolean metadata) {
      return client.get(path + "?action=entries&metadata=" + metadata + "&limit=" + limit, Collections.emptyMap(), HttpResponse.BodyHandlers::ofInputStream);
   }

   @Override
   public CompletionStage<RestResponse> keys(String mediaType) {
      return client.get(path + "?action=keys", Map.of(RestHeaders.ACCEPT, mediaType), HttpResponse.BodyHandlers::ofInputStream);

   }

   @Override
   public CompletionStage<RestResponse> configuration(String mediaType) {
      return client.get(path + "?action=config", mediaType != null ? Map.of(RestHeaders.ACCEPT, mediaType) : Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> stats() {
      return client.get(path + "?action=stats");
   }

   @Override
   public CompletionStage<RestResponse> statsReset() {
      return client.post(path + "?action=stats-reset");
   }

   @Override
   public CompletionStage<RestResponse> distribution() {
      return client.get(path + "?action=distribution");
   }

   @Override
   public CompletionStage<RestResponse> distribution(String key) {

      return client.get(path + "/" + sanitize(key) + "?action=distribution");

   }

   @Override
   public CompletionStage<RestResponse> query(String query, boolean local) {

      return client.get(path + "?action=search&query=" + sanitize(query) + "&local=" + local);

   }

   @Override
   public CompletionStage<RestResponse> deleteByQuery(String query, boolean local) {
      return client.delete(path + "?action=deleteByQuery&query=" + sanitize(query) + "&local=" + local);
   }

   @Override
   public CompletionStage<RestResponse> query(String query, int maxResults, int offset) {
      return query(query, maxResults, offset, -1);
   }

   @Override
   public CompletionStage<RestResponse> query(String query, int maxResults, int offset, int hitCountAccuracy) {
      StringBuilder sb = new StringBuilder(path).append("?action=search&query=").append(sanitize(query));
      if (maxResults > 0) {
         sb.append("&max_results=").append(maxResults);
      }
      if (offset >= 0) {
         sb.append("&offset=").append(offset);
      }
      if (hitCountAccuracy >= 0) {
         sb.append("&hit_count_accuracy=").append(hitCountAccuracy);
      }
      return client.get(sb.toString());
   }

   @Override
   public CompletionStage<RestResponse> xsiteBackups() {

      return client.get(String.format("%s/x-site/backups/", path));

   }

   @Override
   public CompletionStage<RestResponse> backupStatus(String site) {

      return client.get(String.format("%s/x-site/backups/%s", path, site));

   }

   @Override
   public CompletionStage<RestResponse> takeSiteOffline(String site) {
      return executeXSiteOperation(site, "take-offline");
   }

   @Override
   public CompletionStage<RestResponse> bringSiteOnline(String site) {
      return executeXSiteOperation(site, "bring-online");
   }

   @Override
   public CompletionStage<RestResponse> pushSiteState(String site) {
      return executeXSiteOperation(site, "start-push-state");
   }

   @Override
   public CompletionStage<RestResponse> cancelPushState(String site) {
      return executeXSiteOperation(site, "cancel-push-state");
   }

   @Override
   public CompletionStage<RestResponse> cancelReceiveState(String site) {
      return client.post(String.format("%s/x-site/backups/%s?action=%s", path, site, "cancel-receive-state"));
   }

   @Override
   public CompletionStage<RestResponse> pushStateStatus() {
      return client.get(String.format("%s/x-site/backups?action=push-state-status", path));
   }

   @Override
   public CompletionStage<RestResponse> clearPushStateStatus() {
      return client.post(String.format("%s/x-site/local?action=clear-push-state-status", path));

   }

   @Override
   public CompletionStage<RestResponse> getXSiteTakeOfflineConfig(String site) {
      return client.get(String.format("%s/x-site/backups/%s/take-offline-config", path, site));

   }

   @Override
   public CompletionStage<RestResponse> updateXSiteTakeOfflineConfig(String site, int afterFailures, long minTimeToWait) {
      String url = String.format("%s/x-site/backups/%s/take-offline-config", path, site);
      String body = String.format("{\"after_failures\":%d,\"min_wait\":%d}", afterFailures, minTimeToWait);
      return client.put(url, RestEntity.create(MediaType.APPLICATION_JSON, body));
   }

   @Override
   public CompletionStage<RestResponse> xSiteStateTransferMode(String site) {
      return client.get(String.format("%s/x-site/backups/%s/state-transfer-mode", path, site));
   }

   @Override
   public CompletionStage<RestResponse> xSiteStateTransferMode(String site, XSiteStateTransferMode mode) {
      return client.post(String.format("%s/x-site/backups/%s/state-transfer-mode?action=set&mode=%s", path, site, mode.toString()));
   }

   private CompletionStage<RestResponse> executeXSiteOperation(String site, String action) {
      return client.post(String.format("%s/x-site/backups/%s?action=%s", path, site, action));
   }

   @Override
   public CompletionStage<RestResponse> reindex() {
      return executeIndexOperation("reindex", false);
   }

   @Override
   public CompletionStage<RestResponse> reindexLocal() {
      return executeIndexOperation("reindex", true);
   }

   @Override
   public CompletionStage<RestResponse> clearIndex() {
      return executeIndexOperation("clear", false);
   }

   @Override
   public CompletionStage<RestResponse> updateIndexSchema() {
      return executeIndexOperation("updateSchema", false);
   }

   @Override
   public CompletionStage<RestResponse> queryStats() {
      return executeSearchStatOperation("query", null);
   }

   @Override
   public CompletionStage<RestResponse> indexStats() {
      return executeSearchStatOperation("indexes", null);
   }

   @Override
   public CompletionStage<RestResponse> clearQueryStats() {
      return executeSearchStatOperation("query", "clear");
   }

   private CompletionStage<RestResponse> executeIndexOperation(String action, boolean local) {
      return client.post(String.format("%s/search/indexes?action=%s&local=%s", path, action, local));
   }

   private CompletionStage<RestResponse> executeSearchStatOperation(String type, String action) {
      String url = String.format("%s/search/%s/stats", path, type);
      if (action != null) {
         url = url + "?action=" + action;
         return client.post(url);
      } else {
         return client.get(url);
      }
   }

   @Override
   public CompletionStage<RestResponse> details() {
      return client.get(path);
   }

   @Override
   public CompletionStage<RestResponse> indexMetamodel() {
      return client.get(path + "/search/indexes/metamodel");
   }

   @Override
   public CompletionStage<RestResponse> searchStats() {

      return client.get(path + "/search/stats");

   }

   @Override
   public CompletionStage<RestResponse> clearSearchStats() {
      return client.post(path + "/search/stats?action=clear");
   }

   @Override
   public CompletionStage<RestResponse> enableRebalancing() {
      return setRebalancing(true);
   }

   @Override
   public CompletionStage<RestResponse> disableRebalancing() {
      return setRebalancing(false);
   }

   private CompletionStage<RestResponse> setRebalancing(boolean enable) {
      String action = enable ? "enable-rebalancing" : "disable-rebalancing";
      return client.post(path + "?action=" + action);
   }

   @Override
   public CompletionStage<RestResponse> updateWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags) {
      return client.put(path, enums("flags", flags), configuration);
   }

   @Override
   public CompletionStage<RestResponse> updateConfigurationAttribute(String attribute, String... value) {
      StringBuilder sb = new StringBuilder(path);
      sb.append("?action=set-mutable-attribute&attribute-name=");
      sb.append(sanitize(attribute));
      for (String v : value) {
         sb.append("&attribute-value=");
         sb.append(sanitize(v));
      }
      return client.post(sb.toString());
   }

   @Override
   public CompletionStage<RestResponse> configurationAttributes() {
      return configurationAttributes(false);
   }

   @Override
   public CompletionStage<RestResponse> configurationAttributes(boolean full) {
      return client.get(path + "?action=get-mutable-attributes" + (full ? "&full=true" : ""));
   }

   @Override
   public CompletionStage<RestResponse> assignAlias(String alias) {
      return client.post(path + "?action=assign-alias&alias=" + sanitize(alias));
   }

   @Override
   public CompletionStage<RestResponse> getAvailability() {
      return client.get(path + "?action=get-availability");
   }

   @Override
   public CompletionStage<RestResponse> setAvailability(String availability) {
      return client.post(path + "?action=set-availability&availability=" + availability);
   }

   @Override
   public CompletionStage<RestResponse> markTopologyStable(boolean force) {
      return client.post(path + "?action=initialize&force=" + force);
   }

   private Map<String, String> enums(String name, Enum<?>... values) {
      Map<String, String> headers = new HashMap<>();
      if (values != null && values.length > 0) {
         StringJoiner joined = new StringJoiner(" ");
         for (Enum<?> value : values) {
            joined.add(value.name());
         }
         headers.put(name, joined.toString());
      }
      return headers;
   }

   private Map<String, String> expiration(long ttl, long maxIdle) {
      return expiration(new HashMap<>(), ttl, maxIdle);
   }

   private Map<String, String> expiration(Map<String, String> headers, long ttl, long maxIdle) {
      if (ttl != 0) {
         headers.put("timeToLiveSeconds", Long.toString(ttl));
      }
      if (maxIdle != 0) {
         headers.put("maxIdleTimeSeconds", Long.toString(maxIdle));
      }
      return headers;
   }
}
