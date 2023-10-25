package org.infinispan.client.rest.impl.jdk;

import java.io.File;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.MultiPartRestEntity;
import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

/**
 * @since 15.0
 **/
public class RestContainerClientJDK implements RestContainerClient {
   private final RestRawClientJDK client;
   private final String path;

   RestContainerClientJDK(RestRawClientJDK client) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/container";
   }

   @Override
   public CompletionStage<RestResponse> shutdown() {
      return client.post(path + "?action=shutdown");
   }
   @Override
   public CompletionStage<RestResponse> globalConfiguration(String mediaType) {
      return client.get(path + "/config", Map.of(RestClientJDK.ACCEPT, mediaType));
   }

   @Override
   public CompletionStage<RestResponse> cacheConfigurations() {
      return client.get(path + "/cache-configs");
   }

   @Override
   public CompletionStage<RestResponse> cacheConfigurations(String mediaType) {
      return client.get(path + "/cache-configs", Map.of(RestClientJDK.ACCEPT, mediaType));
   }

   @Override
   public CompletionStage<RestResponse> templates(String mediaType) {
      return client.get(path + "/cache-configs/templates", Map.of(RestClientJDK.ACCEPT, mediaType));
   }

   @Override
   public CompletionStage<RestResponse> info() {
      return client.get(path);
   }

   @Override
   public CompletionStage<RestResponse> stats() {
      return client.get(path + "/stats");
   }

   @Override
   public CompletionStage<RestResponse> statsReset() {
      return client.post(path + "/stats?action=reset");
   }

   @Override
   public CompletionStage<RestResponse> backupStatuses() {
      return client.get(path + "/x-site/backups");
   }

   @Override
   public CompletionStage<RestResponse> backupStatus(String site) {
      return client.get(path + "/x-site/backups/" + site);
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
      return client.post(path + "/x-site/backups/" + backup + "?action=" + operation);
   }

   @Override
   public CompletionStage<RestResponse> health() {
      return health(false);
   }

   @Override
   public CompletionStage<RestResponse> health(boolean skipBody) {
      return skipBody ? client.head(path + "/health") : client.get(path + "/health");
   }

   @Override
   public CompletionStage<RestResponse> healthStatus() {
      return client.get(path + "/health/status");
   }

   @Override
   public CompletionStage<RestResponse> createBackup(String name, String workingDir, Map<String, List<String>> resources) {
      Json json = Json.object();
      if (workingDir != null)
         json.set("directory", workingDir);

      if (resources != null)
         json.set("resources", Json.factory().make(resources));
      return client.post(backup(name), RestEntity.create(MediaType.APPLICATION_JSON, json.toString()));
   }

   @Override
   public CompletionStage<RestResponse> getBackup(String name, boolean skipBody) {
      if (skipBody) {
         return client.head(backup(name));
      } else {
         return client.get(backup(name), Map.of(RestClientJDK.ACCEPT, MediaType.APPLICATION_OCTET_STREAM_TYPE), HttpResponse.BodyHandlers::ofInputStream);
      }
   }

   @Override
   public CompletionStage<RestResponse> getBackupNames() {
      return client.get(path + "/backups");
   }

   @Override
   public CompletionStage<RestResponse> deleteBackup(String name) {
      return client.delete(backup(name));
   }

   @Override
   public CompletionStage<RestResponse> restore(String name, File backup, Map<String, List<String>> resources) {
      Json json = resources != null ? Json.factory().make(resources) : Json.object();
      MultiPartRestEntity body = RestEntity.multiPart();
      body.addPart("backup", backup.toPath(), MediaType.APPLICATION_ZIP);
      body.addPart("resources", json.toString());
      return client.post(restore(name), body);
   }

   @Override
   public CompletionStage<RestResponse> restore(String name, String backupLocation, Map<String, List<String>> resources) {
      Json json = Json.object();
      json.set("location", backupLocation);
      if (resources != null)
         json.set("resources", Json.factory().make(resources));
      return client.post(restore(name), RestEntity.create(MediaType.APPLICATION_JSON, json.toString()));
   }

   @Override
   public CompletionStage<RestResponse> getRestore(String name) {
      return client.head(restore(name));
   }

   @Override
   public CompletionStage<RestResponse> getRestoreNames() {
      return client.get(path + "/restores");
   }

   @Override
   public CompletionStage<RestResponse> deleteRestore(String name) {
      return client.delete(restore(name));
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

   private String backup(String name) {
      return path + "/backups/" + name;
   }

   private String restore(String name) {
      return path + "/restores/" + name;
   }
}
