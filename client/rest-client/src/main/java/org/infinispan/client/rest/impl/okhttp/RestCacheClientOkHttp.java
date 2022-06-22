package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.TEXT_PLAIN;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.addEnumHeader;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.configuration.cache.XSiteStateTransferMode;

import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.internal.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestCacheClientOkHttp implements RestCacheClient {
   private final RestClientOkHttp client;
   private final String name;
   private final String cacheUrl;
   private final String rollingUpgradeUrl;

   RestCacheClientOkHttp(RestClientOkHttp restClient, String name) {
      this.client = restClient;
      this.name = name;
      this.cacheUrl = String.format("%s%s/v2/caches/%s", restClient.getBaseURL(), restClient.getConfiguration().contextPath(), sanitize(name));
      this.rollingUpgradeUrl = cacheUrl + "/rolling-upgrade";
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<RestResponse> clear() {
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(cacheUrl + "?action=clear");
      return client.execute(builder);
   }


   @Override
   public CompletionStage<RestResponse> exists() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl).head();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> synchronizeData(Integer readBatch, Integer threads) {
      Request.Builder builder = new Request.Builder();
      StringBuilder sb = new StringBuilder(cacheUrl + "?action=sync-data");
      if (readBatch != null) {
         sb.append("&read-batch=").append(readBatch);
      }
      if (threads != null) {
         sb.append("&threads=").append(threads);
      }
      builder.post(EMPTY_BODY).url(sb.toString());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> synchronizeData() {
      return synchronizeData(null, null);
   }

   @Override
   public CompletionStage<RestResponse> disconnectSource() {
      Request.Builder builder = new Request.Builder();
      builder.url(rollingUpgradeUrl + "/source-connection").delete();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> connectSource(RestEntity value) {
      Request.Builder builder = new Request.Builder();
      builder.post(((RestEntityAdaptorOkHttp) value).toRequestBody()).url(rollingUpgradeUrl + "/source-connection");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> sourceConnected() {
      Request.Builder builder = new Request.Builder();
      builder.url(rollingUpgradeUrl + "/source-connection").head();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> sourceConnection() {
      Request.Builder builder = new Request.Builder();
      builder.url(rollingUpgradeUrl + "/source-connection").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> size() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=size").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String key, String value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).post(RequestBody.create(TEXT_PLAIN, value));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String key, RestEntity value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).post(((RestEntityAdaptorOkHttp) value).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String key, String value, long ttl, long maxIdle) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).post(RequestBody.create(TEXT_PLAIN, value));
      addExpirationHeaders(builder, ttl, maxIdle);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String key, RestEntity value, long ttl, long maxIdle) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).post(((RestEntityAdaptorOkHttp) value).toRequestBody());
      addExpirationHeaders(builder, ttl, maxIdle);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(RequestBody.create(TEXT_PLAIN, value));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value) {
      Request.Builder builder = new Request.Builder();
      if (keyContentType != null) {
         builder.addHeader("Key-Content-Type", keyContentType);
      }
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      if (keyContentType != null) {
         builder.addHeader("Key-Content-Type", keyContentType);
      }
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      headers.forEach(builder::header);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value, long ttl, long maxIdle) {
      Request.Builder builder = new Request.Builder();
      if (keyContentType != null) {
         builder.addHeader("Key-Content-Type", keyContentType);
      }
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      addExpirationHeaders(builder, ttl, maxIdle);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, RestEntity value, String... flags) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      if (flags.length > 0) {
         builder.header("flags", String.join(",", flags));
      }
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, RestEntity value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String value, long ttl, long maxIdle) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(RequestBody.create(TEXT_PLAIN, value));
      addExpirationHeaders(builder, ttl, maxIdle);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, RestEntity value, long ttl, long maxIdle) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      addExpirationHeaders(builder, ttl, maxIdle);
      return client.execute(builder);
   }

   private void addExpirationHeaders(Request.Builder builder, long ttl, long maxIdle) {
      if (ttl != 0) {
         builder.addHeader("timeToLiveSeconds", Long.toString(ttl));
      }
      if (maxIdle != 0) {
         builder.addHeader("maxIdleTimeSeconds", Long.toString(maxIdle));
      }
   }

   @Override
   public CompletionStage<RestResponse> get(String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> get(String key, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key));
      headers.forEach(builder::header);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> get(String key, String mediaType) {
      return get(key, mediaType, false);
   }

   @Override
   public CompletionStage<RestResponse> get(String key, String mediaType, boolean extended) {
      Request.Builder builder = new Request.Builder();
      String url = cacheUrl + "/" + sanitize(key);
      if (extended) {
         url = url + "?extended=true";
      }
      builder.url(url);
      builder.header("Accept", mediaType);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> head(String key) {
      return head(key, Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> head(String key, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key) + "?extended").head();
      headers.forEach(builder::header);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> remove(String key) {
      return remove(key, Collections.emptyMap());
   }

   @Override
   public CompletionStage<RestResponse> remove(String key, Map<String, String> headers) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key) + "?extended").delete();
      headers.forEach(builder::header);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> createWithTemplate(String template, CacheContainerAdmin.AdminFlag... flags) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?template=" + template).post(EMPTY_BODY);
      addEnumHeader("flags", builder, flags);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> createWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl).post(((RestEntityAdaptorOkHttp) configuration).toRequestBody());
      addEnumHeader("flags", builder, flags);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> delete() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl).delete();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> keys() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=keys").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> keys(int limit) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=keys&limit=" + limit).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> entries() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=entries").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> entries(boolean contentNegotiation) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=entries&content-negotiation=" + contentNegotiation).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> entries(int limit) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=entries&limit=" + limit).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> entries(int limit, boolean metadata) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=entries&metadata=" + metadata + "&limit=" + limit).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> keys(String mediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=keys").get();
      builder.header("Accept", mediaType);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> configuration(String mediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=config");
      if (mediaType != null) {
         builder.header("Accept", mediaType);
      }
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> stats() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=stats").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> distribution() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=distribution").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> distribution(String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key) + "?action=distribution").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> query(String query, boolean local) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=search&query=" + sanitize(query) + "&local=" + local).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> query(String query, int maxResults, int offset) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s?action=search&query=%s&max_results=%d&offset=%d", cacheUrl, sanitize(query), maxResults, offset)).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> xsiteBackups() {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups/", cacheUrl));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> backupStatus(String site) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups/%s", cacheUrl, site));
      return client.execute(builder);
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
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(String.format("%s/x-site/backups/%s?action=%s", cacheUrl, site, "cancel-receive-state"));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> pushStateStatus() {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups?action=push-state-status", cacheUrl));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> clearPushStateStatus() {
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(String.format("%s/x-site/local?action=clear-push-state-status", cacheUrl));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> getXSiteTakeOfflineConfig(String site) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups/%s/take-offline-config", cacheUrl, site));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> updateXSiteTakeOfflineConfig(String site, int afterFailures, long minTimeToWait) {
      Request.Builder builder = new Request.Builder();
      String url = String.format("%s/x-site/backups/%s/take-offline-config", cacheUrl, site);
      String body = String.format("{\"after_failures\":%d,\"min_wait\":%d}", afterFailures, minTimeToWait);
      builder.url(url);
      builder.method("PUT", RequestBody.create(MediaType.parse("application/json"), body));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> xSiteStateTransferMode(String site) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups/%s/state-transfer-mode", cacheUrl, site));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> xSiteStateTransferMode(String site, XSiteStateTransferMode mode) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups/%s/state-transfer-mode?action=set&mode=%s", cacheUrl, site, mode.toString()));
      return client.execute(builder.post(EMPTY_BODY));
   }

   private CompletionStage<RestResponse> executeXSiteOperation(String site, String action) {
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(String.format("%s/x-site/backups/%s?action=%s", cacheUrl, site, action));
      return client.execute(builder);
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
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(String.format("%s/search/indexes?action=%s&local=%s", cacheUrl, action, local));
      return client.execute(builder);
   }

   private CompletionStage<RestResponse> executeSearchStatOperation(String type, String action) {
      Request.Builder builder = new Request.Builder();
      String url = String.format("%s/search/%s/stats", cacheUrl, type);
      if (action != null) {
         url = url + "?action=" + action;
         builder.post(EMPTY_BODY);
      }
      builder.url(url);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> details() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> searchStats() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/search/stats");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> clearSearchStats() {
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(cacheUrl + "/search/stats?action=clear");
      return client.execute(builder);
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
      return client.execute(
            new Request.Builder()
                  .post(Util.EMPTY_REQUEST)
                  .url(cacheUrl + "?action=" + action)
      );
   }

   @Override
   public CompletionStage<RestResponse> updateWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl).put(((RestEntityAdaptorOkHttp) configuration).toRequestBody());
      addEnumHeader("flags", builder, flags);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> updateConfigurationAttribute(String attribute, String value) {
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(cacheUrl + "?action=set-mutable-attribute&attribute-name=" + attribute + "&attribute-value=" + value);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> configurationAttributes() {
      return configurationAttributes(false);
   }

   @Override
   public CompletionStage<RestResponse> configurationAttributes(boolean full) {
      Request.Builder builder = new Request.Builder();
      builder.get().url(cacheUrl + "?action=get-mutable-attributes" + (full ? "&full=true" : ""));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> getAvailability() {
      return client.execute(new Request.Builder().url(cacheUrl + "?action=get-availability"));
   }

   @Override
   public CompletionStage<RestResponse> setAvailability(String availability) {
      return client.execute(
            new Request.Builder()
                  .post(EMPTY_BODY)
                  .url(cacheUrl + "?action=set-availability&availability=" + availability)
      );
   }
}
