package org.infinispan.client.rest.impl.jdk;


import static org.infinispan.client.rest.impl.jdk.RestClientJDK.sanitize;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient;

/**
 * @since 14.0
 **/
public class RestTaskClientJDK implements RestTaskClient {
   private final RestRawClientJDK client;
   private final String path;

   RestTaskClientJDK(RestRawClientJDK client) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/tasks";
   }

   @Override
   public CompletionStage<RestResponse> list(ResultType resultType) {
      return client.get(path + "?type=" + resultType.toString());
   }

   @Override
   public CompletionStage<RestResponse> exec(String taskName, String cacheName, Map<String, ?> parameters) {
      Objects.requireNonNull(taskName);
      Objects.requireNonNull(parameters);
      StringBuilder sb = new StringBuilder(path).append('/').append(taskName);
      sb.append("?action=exec");
      if (cacheName != null) {
         sb.append("&cache=");
         sb.append(cacheName);
      }
      for (Map.Entry<String, ?> parameter : parameters.entrySet()) {
         sb.append("&param.");
         sb.append(parameter.getKey());
         sb.append('=');
         sb.append(sanitize(parameter.getValue().toString()));
      }
      return client.post(sb.toString());
   }

   @Override
   public CompletionStage<RestResponse> uploadScript(String taskName, RestEntity script) {
      return client.post(path + "/" + sanitize(taskName), script);
   }

   @Override
   public CompletionStage<RestResponse> downloadScript(String taskName) {
      return client.get(path + "/" + sanitize(taskName) + "?action=script");
   }
}
