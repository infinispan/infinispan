package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestCounterClient {
   CompletionStage<RestResponse> create();

   CompletionStage<RestResponse> delete();

   CompletionStage<RestResponse> configuration();

   CompletionStage<RestResponse> get();

   CompletionStage<RestResponse> add(long delta);

   CompletionStage<RestResponse> increment();

   CompletionStage<RestResponse> decrement();

   CompletionStage<RestResponse> reset();
}
