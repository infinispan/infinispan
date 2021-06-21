package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.internal.Util;

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
   public CompletionStage<RestResponse> globalConfiguration(String mediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseCacheManagerUrl + "/config").header("Accept", mediaType);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> cacheConfigurations() {
      return client.execute(baseCacheManagerUrl, "cache-configs");
   }

   @Override
   public CompletionStage<RestResponse> cacheConfigurations(String mediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseCacheManagerUrl + "/cache-configs").header("Accept", mediaType);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> templates(String mediaType) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseCacheManagerUrl + "/cache-configs/templates").header("Accept", mediaType);
      return client.execute(builder);
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
      Request.Builder builder = new Request.Builder();
      builder.post(EMPTY_BODY).url(String.format("%s/x-site/backups/%s?action=%s", baseCacheManagerUrl, backup, operation));
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> health() {
      return health(false);
   }

   @Override
   public CompletionStage<RestResponse> health(boolean skipBody) {
      Request.Builder builder = new Request.Builder().url(baseCacheManagerUrl);
      if (skipBody) {
         builder.head();
      }
      builder.url(baseCacheManagerUrl + "/health");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> healthStatus() {
      return client.execute(baseCacheManagerUrl, "health", "status");
   }

   @Override
   public CompletionStage<RestResponse> caches() {
      return client.execute(baseCacheManagerUrl, "caches");
   }

   @Override
   public CompletionStage<RestResponse> createBackup(String name, String workingDir, Map<String, List<String>> resources) {
      Json json = Json.object();
      if (workingDir != null)
         json.set("directory", workingDir);

      if (resources != null)
         json.set("resources", Json.factory().make(resources));

      RequestBody body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, json.toString()).toRequestBody();
      Request.Builder builder = backup(name).post(body);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> getBackup(String name, boolean skipBody) {
      Request.Builder builder = backup(name);
      if (skipBody)
         builder.head();

      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> getBackupNames() {
      Request.Builder builder = new Request.Builder().url(baseCacheManagerUrl + "/backups");
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> deleteBackup(String name) {
      return client.execute(backup(name).delete());
   }

   @Override
   public CompletionStage<RestResponse> restore(String name, File backup, Map<String, List<String>> resources) {
      Json json = resources != null ? Json.factory().make(resources) : Json.object();
      RequestBody zipBody = new FileRestEntityOkHttp(MediaType.APPLICATION_ZIP, backup).toRequestBody();

      RequestBody multipartBody = new MultipartBody.Builder()
            .addFormDataPart("resources", json.toString())
            .addFormDataPart("backup", backup.getName(), zipBody)
            .setType(MultipartBody.FORM)
            .build();

      Request.Builder builder = restore(name).post(multipartBody);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> restore(String name, String backupLocation, Map<String, List<String>> resources) {
      Json json = Json.object();
      json.set("location", backupLocation);

      if (resources != null)
         json.set("resources", Json.factory().make(resources));

      RequestBody body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, json.toString()).toRequestBody();
      Request.Builder builder = restore(name).post(body);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> getRestore(String name) {
      return client.execute(restore(name).head());
   }

   @Override
   public CompletionStage<RestResponse> getRestoreNames() {
      return client.execute(new Request.Builder().url(baseCacheManagerUrl + "/restores"));
   }

   @Override
   public CompletionStage<RestResponse> deleteRestore(String name) {
      return client.execute(restore(name).delete());
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
                  .url(baseCacheManagerUrl + "?action=" + action)
      );
   }

   private Request.Builder backup(String name) {
      return new Request.Builder().url(baseCacheManagerUrl + "/backups/" + name);
   }

   private Request.Builder restore(String name) {
      return new Request.Builder().url(baseCacheManagerUrl + "/restores/" + name);
   }
}
