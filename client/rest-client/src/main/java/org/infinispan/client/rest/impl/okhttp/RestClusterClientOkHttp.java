package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;

import okhttp3.MultipartBody;
import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestClusterClientOkHttp implements RestClusterClient {
   private final RestClientOkHttp client;
   private final String baseClusterURL;

   RestClusterClientOkHttp(RestClientOkHttp restClient) {
      this.client = restClient;
      this.baseClusterURL = String.format("%s%s/v2/cluster", restClient.getBaseURL(), restClient.getConfiguration().contextPath()).replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> stop() {
      return stop(Collections.emptyList());
   }

   @Override
   public CompletionStage<RestResponse> stop(List<String> servers) {
      Request.Builder builder = new Request.Builder();
      StringBuilder sb = new StringBuilder(baseClusterURL);
      sb.append("?action=stop");
      for (String server : servers) {
         sb.append("&server=");
         sb.append(server);
      }
      builder.post(EMPTY_BODY).url(sb.toString());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> createBackup(String name) {
      RequestBody body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, Json.object().toString()).toRequestBody();
      return client.execute(backup(name).post(body));
   }

   @Override
   public CompletionStage<RestResponse> getBackup(String name, boolean skipBody) {
      Request.Builder builder = backup(name);
      if (skipBody)
         builder.head();

      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> deleteBackup(String name) {
      return client.execute(backup(name).delete());
   }

   @Override
   public CompletionStage<RestResponse> restore(File backup) {
      RequestBody zipBody = new FileRestEntityOkHttp(MediaType.APPLICATION_ZIP, backup).toRequestBody();

      RequestBody multipartBody = new MultipartBody.Builder()
            .addFormDataPart("backup", backup.getName(), zipBody)
            .setType(MultipartBody.FORM)
            .build();

      Request.Builder builder = restore().post(multipartBody);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> restore(String backupLocation) {
      Json json = Json.object();
      json.set("location", backupLocation);
      RequestBody body = new StringRestEntityOkHttp(MediaType.APPLICATION_JSON, json.toString()).toRequestBody();
      Request.Builder builder = restore().post(body);
      return client.execute(builder);
   }

   private Request.Builder backup(String name) {
      return new Request.Builder().url(baseClusterURL + "/backups/" + name);
   }

   private Request.Builder restore() {
      return new Request.Builder().url(baseClusterURL + "/backups?action=restore");
   }
}
