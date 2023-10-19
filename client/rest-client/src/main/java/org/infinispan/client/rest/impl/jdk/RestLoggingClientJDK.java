package org.infinispan.client.rest.impl.jdk;


import static org.infinispan.client.rest.impl.jdk.RestClientJDK.sanitize;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestLoggingClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class RestLoggingClientJDK implements RestLoggingClient {

   private final RestRawClientJDK client;
   private final String path;

   RestLoggingClientJDK(RestRawClientJDK restClient) {
      this.client = restClient;
      this.path = restClient.getConfiguration().contextPath() + "/v2/logging";
   }

   @Override
   public CompletionStage<RestResponse> listLoggers() {
      return client.get(path + "/loggers");
   }

   @Override
   public CompletionStage<RestResponse> listAppenders() {
      return client.get(path + "/appenders");
   }

   @Override
   public CompletionStage<RestResponse> setLogger(String name, String level, String... appenders) {
      StringBuilder sb = new StringBuilder(path);
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
      return client.put(sb.toString());
   }

   @Override
   public CompletionStage<RestResponse> removeLogger(String name) {
      return client.delete(path + "/loggers/" + sanitize(name));
   }
}
