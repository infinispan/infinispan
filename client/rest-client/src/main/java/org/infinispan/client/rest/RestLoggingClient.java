package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public interface RestLoggingClient {
   CompletionStage<RestResponse> listLoggers();

   CompletionStage<RestResponse> listAppenders();

   CompletionStage<RestResponse> setLogger(String name, String level, String... appenders);

   CompletionStage<RestResponse> removeLogger(String name);
}
