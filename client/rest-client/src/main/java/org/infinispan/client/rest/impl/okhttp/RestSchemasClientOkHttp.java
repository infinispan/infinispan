package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.TEXT_PLAIN;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;

import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * @since 11.0
 */
public class RestSchemasClientOkHttp implements RestSchemaClient {

   private final RestClientOkHttp client;
   private final String baseUrl;

   public RestSchemasClientOkHttp(RestClientOkHttp client) {
      this.client = client;
      this.baseUrl = String.format("%s%s/v2/schemas", client.getBaseURL(), client.getConfiguration().contextPath())
            .replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> names() {
      return client.execute(baseUrl);
   }

   @Override
   public CompletionStage<RestResponse> post(String schemaName, String schemaContents) {
      Request.Builder builder = new Request.Builder();
      RequestBody body = RequestBody.create(TEXT_PLAIN, schemaContents);
      builder.url(schemaUrl(schemaName)).post(body);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> post(String schemaName, RestEntity schemaContents) {
      Request.Builder builder = new Request.Builder();
      builder.url(schemaUrl(schemaName)).post(((RestEntityAdaptorOkHttp)schemaContents).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String schemaName, String schemaContents) {
      Request.Builder builder = new Request.Builder();
      RequestBody body = RequestBody.create(TEXT_PLAIN, schemaContents);
      builder.url(schemaUrl(schemaName)).put(body);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> put(String schemaName, RestEntity schemaContents) {
      Request.Builder builder = new Request.Builder();
      builder.url(schemaUrl(schemaName)).put(((RestEntityAdaptorOkHttp)schemaContents).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> delete(String schemaName) {
      Request.Builder builder = new Request.Builder();
      builder.url(schemaUrl(schemaName)).delete();
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> get(String schemaName) {
      Request.Builder builder = new Request.Builder();
      builder.header("Accept", TEXT_PLAIN_TYPE);
      return client.execute(schemaUrl(schemaName));
   }

   private String schemaUrl(String name) {
      return baseUrl + "/" + sanitize(name);
   }
}
