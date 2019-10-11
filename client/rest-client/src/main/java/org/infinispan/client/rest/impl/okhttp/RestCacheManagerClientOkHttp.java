package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestCacheManagerClientOkHttp implements RestCacheManagerClient {
   private final RestClientOkHttp client;
   private final String name;
   private final String baseCacheManagerUrl;

   RestCacheManagerClientOkHttp(RestClientOkHttp client, String name) {
      this.client = client;
      this.name = name;
      this.baseCacheManagerUrl = String.format("%s%s/v2/cache-managers/%s", client.getBaseURL(), client.getConfiguration().contextPath(), sanitize(name)).replaceAll("//", "/");
   }

   @Override
   public String name() {
      return name;
   }

   @Override
   public CompletionStage<RestResponse> globalConfiguration() {
      return client.execute(baseCacheManagerUrl, "config");
   }

   @Override
   public CompletionStage<RestResponse> cacheConfigurations() {
      return client.execute(baseCacheManagerUrl, "cache-configs");
   }

   @Override
   public CompletionStage<RestResponse> info() {
      return client.execute(baseCacheManagerUrl);
   }

   @Override
   public CompletionStage<RestResponse> stats() {
      return client.execute(baseCacheManagerUrl, "stats");
   }

   @Override
   public CompletionStage<RestResponse> backupStatuses() {
      return client.execute(baseCacheManagerUrl, "x-site", "backups");
   }

   @Override
   public CompletionStage<RestResponse> bringBackupOnline(String backup) {
      return executeXSiteOperation(backup, "bring-online");
   }

   @Override
   public CompletionStage<RestResponse> takeOffline(String backup) {
      return executeXSiteOperation(backup, "take-offline");
   }

   @Override
   public CompletionStage<RestResponse> pushSiteState(String backup) {
      return executeXSiteOperation(backup, "start-push-state");
   }

   @Override
   public CompletionStage<RestResponse> cancelPushState(String backup) {
      return executeXSiteOperation(backup, "cancel-push-state");
   }

   private CompletionStage<RestResponse> executeXSiteOperation(String backup, String operation) {
      String url = String.format("%s/x-site/backups/%s?action=%s", baseCacheManagerUrl, backup, operation);
      return client.execute(url);
   }

   @Override
   public CompletionStage<RestResponse> health() {
      return client.execute(baseCacheManagerUrl, "health");
   }

   @Override
   public CompletionStage<RestResponse> healthStatus() {
      return client.execute(baseCacheManagerUrl, "health", "status");
   }
}
