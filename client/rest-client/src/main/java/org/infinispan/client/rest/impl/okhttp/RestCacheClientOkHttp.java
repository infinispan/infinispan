package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.TEXT_PLAIN;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.addEnumHeader;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestQueryMode;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.api.CacheContainerAdmin;

import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestCacheClientOkHttp implements RestCacheClient {
   private final RestClientOkHttp client;
   private final String name;
   private final String cacheUrl;

   RestCacheClientOkHttp(RestClientOkHttp restClient, String name) {
      this.client = restClient;
      this.name = name;
      this.cacheUrl = String.format("%s%s/v2/caches/%s", restClient.getBaseURL(), restClient.getConfiguration().contextPath(), sanitize(name));
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<RestResponse> clear() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=clear").get();
      return client.execute(builder);
   }


   @Override
   public CompletionStage<RestResponse> exists() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl).head();
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
   public CompletionStage<RestResponse> head(String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key) + "?extended").head();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> remove(String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).delete();
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
   public CompletionStage<RestResponse> configuration() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=config");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> stats() {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=stats").get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> query(String query) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "?action=search&query=" + sanitize(query)).get();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> query(String query, int maxResults, int offset, RestQueryMode queryMode) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s?action=search&query=%s&max_results=%d&offset=%d&query_mode=%s", cacheUrl, sanitize(query), maxResults, offset, queryMode.name())).get();
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
      builder.url(String.format("%s/x-site/backups/%s?action=%s", cacheUrl, site, "cancel-receive-state"));
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
      builder.url(String.format("%s/x-site/local?action=clear-push-state-status", cacheUrl));
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

   private CompletionStage<RestResponse> executeXSiteOperation(String site, String action) {
      Request.Builder builder = new Request.Builder();
      builder.url(String.format("%s/x-site/backups/%s?action=%s", cacheUrl, site, action));
      return client.execute(builder);
   }
}
