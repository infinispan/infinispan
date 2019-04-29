package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
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

   private NettyRestResponse getCounter(RestRequest request) throws RestResponseException {
      try {
         String counterName = request.variables().get("counterName");
         String accept = request.getAcceptHeader();
         MediaType contentType = accept == null ? MediaType.TEXT_PLAIN : negotiateMediaType(accept);
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         CounterConfiguration configuration = counterManager.getConfiguration(counterName);
         if (configuration == null) {
            return responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).entity("Counter not found").build();
         } else {
            responseBuilder.contentType(contentType);
            switch (configuration.type()) {
               case WEAK:
                  long value = counterManager.getWeakCounter(counterName).getValue();
                  responseBuilder.entity(Long.toString(value));
                  break;
               case BOUNDED_STRONG:
               case UNBOUNDED_STRONG:
                  value = counterManager.getStrongCounter(counterName).getValue().get(1, TimeUnit.MINUTES); // FIXME: make non-blocking in ISPN-10210
                  responseBuilder.entity(Long.toString(value));
                  break;
            }
         }
         return responseBuilder.header(HttpHeaderNames.CACHE_CONTROL.toString(), CacheControl.noCache()).build();
      } catch (Exception e) {
         throw new RestResponseException(e);
      }
   }

   private MediaType negotiateMediaType(String accept) {
      return MediaType.parseList(accept).filter(t ->
            t.match(MediaType.TEXT_PLAIN) // TODO: add more types in ISPN-10211
      ).findFirst().orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
   }


   private RestResponse addCounter(RestRequest request) {
      try {
         String counterName = request.variables().get("counterName");
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         CounterConfiguration configuration = counterManager.getConfiguration(counterName);
         if (configuration == null) {
            return responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).entity("Counter not found").build();
         } else {
            ContentSource contents = request.contents();
            long delta;
            if (contents == null) {
               delta = 1;
            } else {
               String s = contents.asString();
               delta = Long.parseLong(s);
            }
            switch (configuration.type()) {
               case WEAK:
                  counterManager.getWeakCounter(counterName).add(delta).get(1, TimeUnit.MINUTES); // FIXME: make non-blocking in ISPN-10210
                  break;
               case BOUNDED_STRONG:
               case UNBOUNDED_STRONG:
                  long value = counterManager.getStrongCounter(counterName).addAndGet(delta).get(1, TimeUnit.MINUTES); // FIXME: make non-blocking in ISPN-10210
                  responseBuilder.entity(Long.toString(value));
                  break;
            }
            return responseBuilder.header(HttpHeaderNames.CACHE_CONTROL.toString(), CacheControl.noCache()).build();
         }
      } catch (Exception e) {
         throw new RestResponseException(e);
      }
   }

   private RestResponse resetCounter(RestRequest request) throws RestResponseException {
      try {
         String counterName = request.variables().get("counterName");
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         CounterConfiguration configuration = counterManager.getConfiguration(counterName);
         if (configuration == null) {
            return responseBuilder.status(HttpResponseStatus.NOT_FOUND.code()).entity("Counter not found").build();
         } else {
            switch (configuration.type()) {
               case WEAK:
                  counterManager.getWeakCounter(counterName).reset().get(1, TimeUnit.MINUTES); // FIXME: make non-blocking in ISPN-10210
                  break;
               case BOUNDED_STRONG:
               case UNBOUNDED_STRONG:
                  counterManager.getStrongCounter(counterName).reset().get(1, TimeUnit.MINUTES); // FIXME: make non-blocking in ISPN-10210
                  break;
            }
         }
         responseBuilder.status(HttpResponseStatus.OK.code());
         return responseBuilder.build();
      } catch (Exception e) {
         throw new RestResponseException(e);
      }
   }
}
