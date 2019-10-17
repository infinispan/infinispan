package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.counter.configuration.StrongCounterConfigurationBuilder;
import org.infinispan.counter.configuration.WeakCounterConfigurationBuilder;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handler for the counter resource.
 *
 * @since 10.0
 */
public class CounterResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;
   private final EmbeddedCounterManager counterManager;

   public CounterResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.counterManager = invocationHelper.getCounterManager();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Lifecycle
            .invocation().methods(POST).path("/v2/counters/{counterName}").handleWith(this::createCounter)
            .invocation().methods(DELETE).path("/v2/counters/{counterName}").handleWith(this::deleteCounter)

            // Config
            .invocation().methods(GET).path("/v2/counters/{counterName}/config").handleWith(this::getConfig)

            // List
            .invocation().methods(GET).path("/v2/counters/").handleWith(this::getCounterNames)

            // Common counter ops
            .invocation().methods(GET).path("/v2/counters/{counterName}").handleWith(this::getCounter)
            .invocation().methods(GET).path("/v2/counters/{counterName}").withAction("reset").handleWith(this::resetCounter)
            .invocation().methods(GET).path("/v2/counters/{counterName}").withAction("increment").handleWith(this::incrementCounter)
            .invocation().methods(GET).path("/v2/counters/{counterName}").withAction("decrement").handleWith(this::decrementCounter)
            .invocation().methods(GET).path("/v2/counters/{counterName}").withAction("add").handleWith(this::addValue)

            // Strong counter ops
            .invocation().methods(GET).path("/v2/counters/{counterName}").withAction("compareAndSet").handleWith(this::compareSet)
            .invocation().methods(GET).path("/v2/counters/{counterName}").withAction("compareAndSwap").handleWith(this::compareSwap)
            .create();
   }

   private CompletionStage<RestResponse> createCounter(RestRequest request) throws RestResponseException {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");

      String contents = request.contents().asString();

      if (contents == null || contents.length() == 0) {
         responseBuilder.status(HttpResponseStatus.BAD_REQUEST);
         responseBuilder.entity("Configuration not provided");
         return completedFuture(responseBuilder.build());
      }
      try {
         CounterConfiguration configuration = createCounterConfiguration(contents);
         if (configuration == null) {
            responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity("Invalid configuration");
            return completedFuture(responseBuilder.build());
         }
         return counterManager.defineCounterAsync(counterName, configuration).thenApply(r -> responseBuilder.build());
      } catch (IOException e) {
         throw new RestResponseException(e.getCause());
      }
   }

   private CompletionStage<RestResponse> deleteCounter(RestRequest request) {
      String counterName = request.variables().get("counterName");

      EmbeddedCounterManager counterManager = invocationHelper.getCounterManager();

      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return completedFuture(new NettyRestResponse.Builder().status(NOT_FOUND).build());
         counterManager.undefineCounter(counterName);
         return completedFuture(new NettyRestResponse.Builder().status(NO_CONTENT).build());
      });
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");
      return invocationHelper.getCounterManager().getConfigurationAsync(counterName).thenApply(cfg -> {
         if (cfg == null) return responseBuilder.status(NOT_FOUND).build();

         AbstractCounterConfiguration parsedConfig = ConvertUtil.configToParsedConfig(counterName, cfg);
         String json = invocationHelper.getJsonWriter().toJSON(parsedConfig);
         return responseBuilder.entity(json).contentType(APPLICATION_JSON).build();
      });
   }

   private CompletionStage<RestResponse> getCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");
      String accept = request.getAcceptHeader();
      MediaType contentType = accept == null ? MediaType.TEXT_PLAIN : negotiateMediaType(accept);
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return completedFuture(responseBuilder.status(NOT_FOUND).build());

         responseBuilder.contentType(contentType).header(CACHE_CONTROL.toString(), CacheControl.noCache());

         CompletionStage<Long> response = configuration.type() == CounterType.WEAK ?
               completedFuture(counterManager.getWeakCounter(counterName).getValue()) :
               counterManager.getStrongCounter(counterName).getValue();

         return response.thenApply(v -> responseBuilder.entity(Long.toString(v)).build());
      });
   }

   private CompletionStage<RestResponse> resetCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");

      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return completedFuture(new NettyRestResponse.Builder().status(NOT_FOUND).build());
         CompletionStage<Void> result = configuration.type() == CounterType.WEAK ?
               counterManager.getWeakCounter(counterName).reset() :
               counterManager.getStrongCounter(counterName).reset();

         return result.thenApply(v -> new NettyRestResponse.Builder().build());
      });
   }

   private CompletionStage<RestResponse> getCounterNames(RestRequest request) throws RestResponseException {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      try {
         byte[] bytes = invocationHelper.getMapper().writeValueAsBytes(counterManager.getCounterNames());
         responseBuilder.contentType(APPLICATION_JSON).entity(bytes).status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> incrementCounter(RestRequest request) {
      return executeCommonCounterOp(request, WeakCounter::increment, StrongCounter::incrementAndGet);
   }

   private CompletionStage<RestResponse> decrementCounter(RestRequest request) {
      return executeCommonCounterOp(request, WeakCounter::decrement, StrongCounter::decrementAndGet);
   }

   private CompletionStage<RestResponse> addValue(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");
      Long delta = checkForNumericParam("delta", request, responseBuilder);
      if (delta == null) return completedFuture(responseBuilder.build());

      CounterConfiguration configuration = counterManager.getConfiguration(counterName);
      if (configuration == null) return completedFuture(responseBuilder.status(NOT_FOUND).build());

      CounterType type = configuration.type();
      if (type == CounterType.WEAK) {
         CompletionStage<WeakCounter> weakCounter = getWeakCounter(counterName);
         return weakCounter.thenCompose(counter -> {
            if (counter == null) {
               responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity(String.format("Weak counter '%s' not found", counterName));
               return completedFuture(responseBuilder.build());
            }
            return counter.add(delta).thenApply(v -> responseBuilder.build());
         });
      } else {
         StrongCounter strongCounter = checkForStrongCounter(counterName, responseBuilder);
         if (strongCounter == null) return completedFuture(responseBuilder.build());
         return strongCounter.addAndGet(delta).thenApply(value -> {
            try {
               byte[] result = invocationHelper.getMapper().writeValueAsBytes(value);
               return responseBuilder.status(OK).entity(result).build();
            } catch (JsonProcessingException e) {
               throw new RestResponseException(e);
            }
         });
      }
   }

   private CompletionStage<RestResponse> compareSet(RestRequest request) {
      return executeCounterCAS(request, StrongCounter::compareAndSet);
   }

   private CompletionStage<RestResponse> compareSwap(RestRequest request) {
      return executeCounterCAS(request, StrongCounter::compareAndSwap);
   }

   private CounterConfiguration createCounterConfiguration(String json) throws IOException {
      JsonNode jsonNode = invocationHelper.getMapper().readValue(json, JsonNode.class);
      boolean strongCounter = jsonNode.has("strong-counter");
      boolean weakCounter = jsonNode.has("weak-counter");

      if (strongCounter) {
         StrongCounterConfigurationBuilder counterBuilder = new StrongCounterConfigurationBuilder(null);
         invocationHelper.getJsonReader().readJson(counterBuilder, json);
         return ConvertUtil.parsedConfigToConfig(counterBuilder.create());
      } else if (weakCounter) {
         WeakCounterConfigurationBuilder counterBuilder = new WeakCounterConfigurationBuilder(null);
         invocationHelper.getJsonReader().readJson(counterBuilder, json);
         return ConvertUtil.parsedConfigToConfig(counterBuilder.create());
      }
      return null;
   }

   private CompletionStage<RestResponse> executeCommonCounterOp(RestRequest request,
                                                                Function<WeakCounter, CompletionStage<Void>> weakOp,
                                                                Function<StrongCounter, CompletableFuture<Long>> strongOp) {
      String counterName = request.variables().get("counterName");
      CounterConfiguration configuration = counterManager.getConfiguration(counterName);
      if (configuration == null) return completedFuture(new NettyRestResponse.Builder().status(NOT_FOUND).build());

      CounterType type = configuration.type();

      if (type == CounterType.WEAK) return executeWeakCounterOp(counterName, weakOp);

      return executeStrongCounterOp(counterName, strongOp);
   }

   private <T> CompletionStage<RestResponse> executeCounterCAS(RestRequest request, CASInvocation<StrongCounter, Long, Long, CompletableFuture<T>> invocation) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");

      Long expect = checkForNumericParam("expect", request, responseBuilder);
      if (expect == null) return completedFuture(responseBuilder.build());

      Long update = checkForNumericParam("update", request, responseBuilder);
      if (update == null) return completedFuture(responseBuilder.build());

      StrongCounter strongCounter = checkForStrongCounter(counterName, responseBuilder);
      if (strongCounter == null) return completedFuture(responseBuilder.build());
      CompletableFuture<T> opResult = invocation.apply(strongCounter, expect, update);

      return opResult.thenCompose(value -> {
         try {
            byte[] result = invocationHelper.getMapper().writeValueAsBytes(value);
            return completedFuture(responseBuilder.status(OK).entity(result).build());
         } catch (JsonProcessingException e) {
            throw new RestResponseException(INTERNAL_SERVER_ERROR, e.getMessage());
         }
      });
   }

   @FunctionalInterface
   interface CASInvocation<C, A, B, R> {
      R apply(C t, A u, B v);
   }

   private CompletionStage<RestResponse> executeStrongCounterOp(String counterName, Function<StrongCounter, CompletableFuture<Long>> op) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      StrongCounter strongCounter = checkForStrongCounter(counterName, responseBuilder);
      if (strongCounter == null) return completedFuture(responseBuilder.status(NOT_FOUND).build());
      return op.apply(strongCounter).thenCompose(value -> {
         try {
            byte[] result = invocationHelper.getMapper().writeValueAsBytes(value);
            return completedFuture(responseBuilder.status(OK).entity(result).build());
         } catch (JsonProcessingException e) {
            throw new RestResponseException(e);
         }
      });
   }

   private CompletionStage<WeakCounter> getWeakCounter(String name) {
      return CompletableFuture.supplyAsync(() -> counterManager.getWeakCounter(name), invocationHelper.getExecutor());
   }

   private StrongCounter checkForStrongCounter(String name, NettyRestResponse.Builder builder) {
      try {
         return counterManager.getStrongCounter(name);
      } catch (Exception e) {
         builder.status(HttpResponseStatus.BAD_REQUEST).entity(String.format("Strong counter '%s' not found", name));
      }
      return null;
   }

   private Long checkForNumericParam(String name, RestRequest request, NettyRestResponse.Builder builder) {
      List<String> params = request.parameters().get(name);
      if (params == null || params.size() != 1) {
         builder.status(HttpResponseStatus.BAD_REQUEST)
               .entity(String.format("A single '%s' param must be provided", name));
      } else {
         try {
            return Long.valueOf(params.iterator().next());
         } catch (NumberFormatException e) {
            builder.status(HttpResponseStatus.BAD_REQUEST)
                  .entity(String.format("Param '%s' must be a number", name));
         }
      }
      return null;
   }

   private CompletionStage<RestResponse> executeWeakCounterOp(String counterName, Function<WeakCounter, CompletionStage<Void>> op) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      CompletionStage<WeakCounter> weakCounter = getWeakCounter(counterName);
      return weakCounter.thenCompose(counter -> {
         if (counter == null) {
            responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity(String.format("Weak counter '%s' not found", counterName));
            return completedFuture(responseBuilder.build());
         }
         return op.apply(counter).thenCompose(t -> completedFuture(responseBuilder.status(OK).build()));
      });
   }

   private MediaType negotiateMediaType(String accept) {
      return MediaType.parseList(accept).filter(t ->
            t.match(MediaType.TEXT_PLAIN) // TODO: add more types in ISPN-10211
      ).findFirst().orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
   }
}
