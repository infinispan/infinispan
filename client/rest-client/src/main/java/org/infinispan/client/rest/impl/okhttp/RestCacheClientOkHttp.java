package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.TEXT_PLAIN;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.addEnumHeader;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.api.CacheContainerAdmin;

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

   public CompletionStage<RestResponse> post(String key, RestEntity value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).post(((RestEntityAdaptorOkHttp) value).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String key, String value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(RequestBody.create(TEXT_PLAIN, value));
      return client.execute(builder);
   }

   public CompletionStage<RestResponse> put(String key, RestEntity value) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key)).put(((RestEntityAdaptorOkHttp) value).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> get(String key) {
      Request.Builder builder = new Request.Builder();
      builder.url(cacheUrl + "/" + sanitize(key));
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
}
