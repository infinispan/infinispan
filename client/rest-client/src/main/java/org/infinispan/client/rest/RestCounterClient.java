package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestCounterClient {
   CompletionStage<RestResponse> create(RestEntity configuration);

   CompletionStage<RestResponse> delete();

   CompletionStage<RestResponse> configuration();

   CompletionStage<RestResponse> configuration(String mediaType);

   CompletionStage<RestResponse> get();

   CompletionStage<RestResponse> add(long delta);

   CompletionStage<RestResponse> increment();

   CompletionStage<RestResponse> decrement();

   CompletionStage<RestResponse> reset();

   CompletionStage<RestResponse> compareAndSet(long expect, long value);

   CompletionStage<RestResponse> compareAndSwap(long expect, long value);

   CompletionStage<RestResponse> getAndSet(long newValue);
}
