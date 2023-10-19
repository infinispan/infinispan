package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.client.rest.impl.jdk.RestClientJDK.ACCEPT;
import static org.infinispan.client.rest.impl.jdk.RestClientJDK.sanitize;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestSchemaClient;

/**
 * @since 14.0
 **/
public class RestSchemaClientJDK implements RestSchemaClient {
   private final RestRawClientJDK client;
   private final String path;

   public RestSchemaClientJDK(RestRawClientJDK client) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/schemas";
   }

   @Override
   public CompletionStage<RestResponse> names() {
      return client.get(path);
   }

   @Override
   public CompletionStage<RestResponse> types() {
      return client.get(path + "?action=types");
   }

   @Override
   public CompletionStage<RestResponse> post(String schemaName, String schemaContents) {
      return client.post(schemaUrl(schemaName), RestEntity.create(TEXT_PLAIN, schemaContents));
   }

   @Override
   public CompletionStage<RestResponse> post(String schemaName, RestEntity schemaContents) {
      return client.post(schemaUrl(schemaName), schemaContents);
   }

   @Override
   public CompletionStage<RestResponse> put(String schemaName, String schemaContents) {
      return client.put(schemaUrl(schemaName), RestEntity.create(TEXT_PLAIN, schemaContents));
   }

   @Override
   public CompletionStage<RestResponse> put(String schemaName, RestEntity schemaContents) {
      return client.put(schemaUrl(schemaName), schemaContents);
   }

   @Override
   public CompletionStage<RestResponse> delete(String schemaName) {
      return client.delete(schemaUrl(schemaName));
   }

   @Override
   public CompletionStage<RestResponse> get(String schemaName) {
      return client.get(schemaUrl(schemaName), Map.of(ACCEPT, TEXT_PLAIN_TYPE));
   }

   private String schemaUrl(String name) {
      return path + "/" + sanitize(name);
   }
}
