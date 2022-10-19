package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.util.concurrent.CompletableFutures.completedExceptionFuture;
import static org.infinispan.commons.util.concurrent.CompletableFutures.extractException;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;
import static org.infinispan.rest.resources.ResourceUtil.noContent;
import static org.infinispan.rest.resources.ResourceUtil.noContentResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.notFoundResponseFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.commons.configuration.io.ConfigurationReader;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.io.StringBuilderWriter;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.counter.configuration.CounterConfigurationSerializer;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.exception.CounterNotFoundException;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.counter.impl.manager.InternalCounterAdmin;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handler for the counter resource.
 *
 * @since 10.0
 */
public class CounterResource implements ResourceHandler {
   private final InvocationHelper invocationHelper;

   public CounterResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
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
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("reset").handleWith(this::resetCounter)
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("increment").handleWith(this::incrementCounter)
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("decrement").handleWith(this::decrementCounter)
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("add").handleWith(this::addValue)

            // Strong counter ops
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("compareAndSet").handleWith(this::compareSet)
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("compareAndSwap").handleWith(this::compareSwap)
            .create();
   }

   private CompletionStage<RestResponse> createCounter(RestRequest request) throws RestResponseException {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");

      String contents = request.contents().asString();

      if (contents == null || contents.isEmpty()) {
         responseBuilder.status(HttpResponseStatus.BAD_REQUEST);
         responseBuilder.entity("Configuration not provided");
         return completedFuture(responseBuilder.build());
      }
      CounterConfiguration configuration = createCounterConfiguration(contents);
      if (configuration == null) {
         responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity("Invalid configuration");
         return completedFuture(responseBuilder.build());
      }

      return invocationHelper.getCounterManager()
            .defineCounterAsync(counterName, configuration)
            .thenApply(r -> responseBuilder.build());
   }

   private CompletionStage<RestResponse> deleteCounter(RestRequest request) {
      String counterName = request.variables().get("counterName");

      EmbeddedCounterManager counterManager = invocationHelper.getCounterManager();

      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return notFoundResponseFuture();
         return counterManager.removeAsync(counterName, false)
               .thenApply(ignore -> new NettyRestResponse.Builder().status(NO_CONTENT).build());
      });
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");
      boolean pretty = Boolean.parseBoolean(request.getParameter("pretty"));
      return invocationHelper.getCounterManager().getConfigurationAsync(counterName).thenApply(cfg -> {
         if (cfg == null) return responseBuilder.status(NOT_FOUND).build();

         AbstractCounterConfiguration parsedConfig = ConvertUtil.configToParsedConfig(counterName, cfg);
         CounterConfigurationSerializer ccs = new CounterConfigurationSerializer();
         StringBuilderWriter sw = new StringBuilderWriter();
         try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(pretty).build()) {
            ccs.serializeConfiguration(w, parsedConfig);
         }
         return responseBuilder.entity(sw.toString()).contentType(APPLICATION_JSON).build();
      });
   }

   private CompletionStage<RestResponse> getCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");
      String accept = request.getAcceptHeader();
      MediaType contentType = accept == null ? MediaType.TEXT_PLAIN : negotiateMediaType(accept);

      EmbeddedCounterManager counterManager = invocationHelper.getCounterManager();
      CompletionStage<InternalCounterAdmin> stage = counterManager.getOrCreateAsync(counterName);
      return CompletionStages.handleAndCompose(stage, (counter, throwable) -> {
         if (throwable != null) {
            return handleThrowable(throwable);
         }
         return counter.value().thenApply(v -> new NettyRestResponse.Builder()
               .contentType(contentType)
               .header(CACHE_CONTROL.toString(), CacheControl.noCache())
               .entity(Long.toString(v)).build());
      });
   }

   private CompletionStage<RestResponse> resetCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");

      EmbeddedCounterManager counterManager = invocationHelper.getCounterManager();
      CompletionStage<InternalCounterAdmin> stage = counterManager.getOrCreateAsync(counterName);
      return CompletionStages.handleAndCompose(stage, (counter, throwable) -> {
         if (throwable != null) {
            return handleThrowable(throwable);
         } else {
            return counter.reset().thenCompose(unused -> noContentResponseFuture());
         }
      });
   }

   private CompletionStage<RestResponse> getCounterNames(RestRequest request) throws RestResponseException {
      return asJsonResponseFuture(Json.make(invocationHelper.getCounterManager().getCounterNames()), isPretty(request));
   }

   private CompletionStage<RestResponse> incrementCounter(RestRequest request) {
      return executeCommonCounterOp(request, WeakCounter::increment, StrongCounter::incrementAndGet);
   }

   private CompletionStage<RestResponse> decrementCounter(RestRequest request) {
      return executeCommonCounterOp(request, WeakCounter::decrement, StrongCounter::decrementAndGet);
   }

   private CompletionStage<RestResponse> addValue(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      Long delta = checkForNumericParam("delta", request, responseBuilder);
      if (delta == null) return completedFuture(responseBuilder.build());

      return executeCommonCounterOp(request, weakCounter -> weakCounter.add(delta), strongCounter -> strongCounter.addAndGet(delta));
   }

   private CompletionStage<RestResponse> compareSet(RestRequest request) {
      return executeCounterCAS(request, StrongCounter::compareAndSet);
   }

   private CompletionStage<RestResponse> compareSwap(RestRequest request) {
      return executeCounterCAS(request, StrongCounter::compareAndSwap);
   }

   private CounterConfiguration createCounterConfiguration(String json) {
      try (ConfigurationReader reader = ConfigurationReader.from(json).withType(APPLICATION_JSON).build()) {
         ConfigurationBuilderHolder holder = invocationHelper.getParserRegistry().parse(reader, new ConfigurationBuilderHolder());
         CounterManagerConfigurationBuilder counterModule = holder.getGlobalConfigurationBuilder().module(CounterManagerConfigurationBuilder.class);
         CounterManagerConfiguration configuration = counterModule.create();
         return ConvertUtil.parsedConfigToConfig(configuration.counters().values().iterator().next());
      }
   }

   private CompletionStage<RestResponse> executeCommonCounterOp(RestRequest request,
                                                                Function<WeakCounter, CompletionStage<Void>> weakOp,
                                                                Function<StrongCounter, CompletableFuture<Long>> strongOp) {
      String counterName = request.variables().get("counterName");
      boolean pretty = isPretty(request);
      CompletionStage<InternalCounterAdmin> stage = invocationHelper.getCounterManager().getOrCreateAsync(counterName);
      return CompletionStages.handleAndCompose(stage, (counter, throwable) -> {
         if (throwable != null) {
            return handleThrowable(throwable);
         }
         CompletableFuture<RestResponse> rsp = new CompletableFuture<>();
         if (counter.isWeakCounter()) {
            weakOp.apply(counter.asWeakCounter()).whenComplete((___, t) -> {
               if (t != null) {
                  rsp.completeExceptionally(t);
               } else {
                  rsp.complete(noContent());
               }
            });
         } else {
            strongOp.apply(counter.asStrongCounter()).whenComplete((rv, t) -> {
               if (t != null) {
                  rsp.completeExceptionally(t);
               } else {
                  rsp.complete(asJsonResponse(Json.make(rv), pretty));
               }
            });
         }
         return rsp;
      });
   }

   private <T> CompletionStage<RestResponse> executeCounterCAS(RestRequest request, CASInvocation<StrongCounter, Long, Long, CompletableFuture<T>> invocation) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");
      boolean pretty = isPretty(request);

      Long expect = checkForNumericParam("expect", request, responseBuilder);
      if (expect == null) return completedFuture(responseBuilder.build());

      Long update = checkForNumericParam("update", request, responseBuilder);
      if (update == null) return completedFuture(responseBuilder.build());

      CompletionStage<StrongCounter> stage = invocationHelper.getCounterManager().getStrongCounterAsync(counterName);
      return CompletionStages.handleAndCompose(stage, (counter, throwable) -> {
         if (throwable != null) {
            return handleThrowable(throwable);
         }
         return invocation.apply(counter, expect, update).thenCompose(value -> asJsonResponseFuture(Json.make(value), pretty));
      });
   }

   @FunctionalInterface
   interface CASInvocation<C, A, B, R> {
      R apply(C t, A u, B v);
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

   private MediaType negotiateMediaType(String accept) {
      return MediaType.parseList(accept).filter(t ->
            t.match(MediaType.TEXT_PLAIN) // TODO: add more types in ISPN-10211
      ).findFirst().orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
   }

   private CompletionStage<RestResponse> handleThrowable(Throwable throwable) {
      Throwable cause = extractException(throwable);
      if (cause instanceof CounterNotFoundException) {
         return notFoundResponseFuture();
      }
      return completedExceptionFuture(cause);
   }
}
