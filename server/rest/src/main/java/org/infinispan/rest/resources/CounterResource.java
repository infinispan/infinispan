package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handler for the counter resource.
 *
 * @since 10.0
 */
public class CounterResource implements ResourceHandler {
   private final CounterManager counterManager;

   public CounterResource(CounterManager counterManager) {
      this.counterManager = counterManager;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(POST).path("/v2/counters/{counterName}").handleWith(this::addCounter)
            .invocation().methods(GET).path("/v2/counters/{counterName}").handleWith(this::getCounter)
            .invocation().methods(DELETE).path("/v2/counters/{counterName}").handleWith(this::resetCounter)
            .create();
   }

   private CompletionStage<RestResponse> getCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");
      String accept = request.getAcceptHeader();
      MediaType contentType = accept == null ? MediaType.TEXT_PLAIN : negotiateMediaType(accept);
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      CounterConfiguration configuration = counterManager.getConfiguration(counterName);
      if (configuration == null) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).entity("Counter not found").build());
      }
      responseBuilder.contentType(contentType).header(HttpHeaderNames.CACHE_CONTROL.toString(), CacheControl.noCache());
      CompletionStage<Long> response;
      if (configuration.type() == CounterType.WEAK) {
         response = CompletableFuture.completedFuture(counterManager.getWeakCounter(counterName).getValue());
      } else {
         response = counterManager.getStrongCounter(counterName).getValue();
      }
      return response.thenApply(res -> responseBuilder.entity(Long.toString(res)).build());
   }

   private MediaType negotiateMediaType(String accept) {
      return MediaType.parseList(accept).filter(t ->
            t.match(MediaType.TEXT_PLAIN) // TODO: add more types in ISPN-10211
      ).findFirst().orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
   }


   private CompletionStage<RestResponse> addCounter(RestRequest request) {
      String counterName = request.variables().get("counterName");
      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();

      CounterConfiguration configuration = counterManager.getConfiguration(counterName);
      if (configuration == null) {
         return CompletableFuture.completedFuture(builder.status(HttpResponseStatus.NOT_FOUND.code()).entity("Counter not found").build());
      }
      builder.header(HttpHeaderNames.CACHE_CONTROL.toString(), CacheControl.noCache());

      ContentSource contents = request.contents();
      long delta;
      if (contents == null) {
         delta = 1;
      } else {
         String s = contents.asString();
         delta = Long.parseLong(s);
      }
      if (configuration.type() == CounterType.WEAK)
         return counterManager.getWeakCounter(counterName).add(delta).thenApply(v -> builder.build());

      return counterManager.getStrongCounter(counterName).addAndGet(delta).thenApply(value -> builder.entity(Long.toString(value)).build());
   }

   private CompletionStage<RestResponse> resetCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      responseBuilder.status(HttpResponseStatus.OK.code());

      CounterConfiguration configuration = counterManager.getConfiguration(counterName);
      if (configuration == null) return CompletableFuture.completedFuture(
            responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).entity("Counter not found").build());

      CompletionStage<Void> result = configuration.type() == CounterType.WEAK ?
            counterManager.getWeakCounter(counterName).reset() :
            counterManager.getStrongCounter(counterName).reset();

      return result.thenApply(v -> responseBuilder.build());
   }
}
