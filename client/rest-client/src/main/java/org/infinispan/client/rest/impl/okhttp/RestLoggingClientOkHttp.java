package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.sanitize;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestLoggingClient;
import org.infinispan.client.rest.RestResponse;

import okhttp3.Request;
import okhttp3.internal.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class RestLoggingClientOkHttp implements RestLoggingClient {

   private final RestClientOkHttp client;
   private final String baseLoggingURL;

   RestLoggingClientOkHttp(RestClientOkHttp restClient) {
      this.client = restClient;
      this.baseLoggingURL = String.format("%s%s/v2/logging", restClient.getBaseURL(), restClient.getConfiguration().contextPath());
   }

   @Override
   public CompletionStage<RestResponse> listLoggers() {
      return client.execute(baseLoggingURL, "loggers");
   }

   @Override
   public CompletionStage<RestResponse> listAppenders() {
      return client.execute(baseLoggingURL, "appenders");
   }

   @Override
   public CompletionStage<RestResponse> setLogger(String name, String level, String... appenders) {
      Request.Builder builder = new Request.Builder();
      StringBuilder sb = new StringBuilder(baseLoggingURL);
      sb.append("/loggers/");
      if (name != null) {
         sb.append(sanitize(name));
      }
      sb.append("?");
      boolean amp = false;
      if (level != null) {
         sb.append("level=").append(level);
         amp = true;
      }
      if (appenders != null) {
         for(String appender : appenders) {
            if (amp) {
               sb.append("&");
            }
            sb.append("appender=").append(appender);
            amp = true;
         }
      }
      builder.url(sb.toString()).put(Util.EMPTY_REQUEST);
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> removeLogger(String name) {
      Request.Builder builder = new Request.Builder();
      builder.url(baseLoggingURL + "/loggers/" + sanitize(name)).delete();
      return client.execute(builder);
   }
}
