package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;
import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestTaskClient;

import okhttp3.Request;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public class RestTaskClientOkHttp implements RestTaskClient {
   private final RestClientOkHttp client;
   private final String baseURL;

   RestTaskClientOkHttp(RestClientOkHttp client) {
      this.client = client;
      this.baseURL = String.format("%s%s/v2/tasks", client.getBaseURL(), client.getConfiguration().contextPath()).replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> list(ResultType resultType) {
      return client.execute(baseURL + "?type=" + resultType.toString());
   }

   @Override
   public CompletionStage<RestResponse> exec(String taskName, String cacheName, Map<String, ?> parameters) {
      Objects.requireNonNull(taskName);
      Objects.requireNonNull(parameters);
      Request.Builder builder = new Request.Builder();
      StringBuilder sb = new StringBuilder(baseURL).append('/').append(taskName);
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
      builder.url(sb.toString()).post(EMPTY_BODY);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> uploadScript(String taskName, RestEntity script) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseURL + "/" + sanitize(taskName)).post(((RestEntityAdaptorOkHttp) script).toRequestBody());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> downloadScript(String taskName) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseURL + "/" + sanitize(taskName) + "?action=script");
      return client.execute(builder);
   }
}
