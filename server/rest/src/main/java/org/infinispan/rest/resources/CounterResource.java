package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.util.concurrent.CompletableFutures.extractException;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

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
import org.infinispan.commons.util.concurrent.CompletionStages;

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
      return new Invocations.Builder("counter", "REST resource to manage counters.")
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
            .invocation().methods(POST).path("/v2/counters/{counterName}").withAction("getAndSet").handleWith(this::getAndSet)
            .create();
   }

   private CompletionStage<RestResponse> createCounter(RestRequest request) throws RestResponseException {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String counterName = request.variables().get("counterName");

      String contents = request.contents().asString();

      if (contents == null || contents.isEmpty()) {
         throw Log.REST.missingContent();
      }
      CounterConfiguration configuration = createCounterConfiguration(contents);
      if (configuration == null) {
         throw Log.REST.invalidContent();
      }

      return invocationHelper.getCounterManager()
            .defineCounterAsync(counterName, configuration)
            .thenApply(created -> created ?
                  responseBuilder.build() :
                  responseBuilder.status(NOT_MODIFIED)
                        .entity("Unable to create counter: " + counterName)
                        .build());
   }

   private CompletionStage<RestResponse> deleteCounter(RestRequest request) {
      String counterName = request.variables().get("counterName");

      EmbeddedCounterManager counterManager = invocationHelper.getCounterManager();

      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
         return counterManager.removeAsync(counterName, false)
               .thenApply(ignore -> invocationHelper.newResponse(request).status(NO_CONTENT).build());
      });
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
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
            return handleThrowable(request, throwable);
         }
         return counter.value().thenApply(v -> invocationHelper.newResponse(request)
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
            return handleThrowable(request, throwable);
         } else {
            return counter.reset().thenCompose(unused -> invocationHelper.newResponse(request, NO_CONTENT).toFuture());
         }
      });
   }

   private CompletionStage<RestResponse> getCounterNames(RestRequest request) throws RestResponseException {
      return asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(invocationHelper.getCounterManager().getCounterNames()), isPretty(request));
   }

   private CompletionStage<RestResponse> incrementCounter(RestRequest request) {
      return executeCommonCounterOp(request, WeakCounter::increment, StrongCounter::incrementAndGet);
   }

   private CompletionStage<RestResponse> decrementCounter(RestRequest request) {
      return executeCommonCounterOp(request, WeakCounter::decrement, StrongCounter::decrementAndGet);
   }

   private CompletionStage<RestResponse> addValue(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
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

   private CompletionStage<RestResponse> getAndSet(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");
      Long value = checkForNumericParam("value", request, responseBuilder);

      if (value == null) return completedFuture(responseBuilder.build());

      CompletionStage<StrongCounter> stage = invocationHelper.getCounterManager().getStrongCounterAsync(counterName);
      return CompletionStages.handleAndCompose(stage, (counter, throwable) -> {
         if (throwable != null) {
            return handleThrowable(request, throwable);
         }
         return counter.getAndSet(value).thenApply(v -> new NettyRestResponse.Builder().status(OK).entity(String.valueOf(v)).build());
      });
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
            return handleThrowable(request, throwable);
         }
         CompletableFuture<RestResponse> rsp = new CompletableFuture<>();
         if (counter.isWeakCounter()) {
            weakOp.apply(counter.asWeakCounter()).whenComplete((___, t) -> {
               if (t != null) {
                  rsp.completeExceptionally(t);
               } else {
                  rsp.complete(invocationHelper.newResponse(request, NO_CONTENT));
               }
            });
         } else {
            strongOp.apply(counter.asStrongCounter()).whenComplete((rv, t) -> {
               if (t != null) {
                  rsp.completeExceptionally(t);
               } else {
                  rsp.complete(asJsonResponse(invocationHelper.newResponse(request), Json.make(rv), pretty));
               }
            });
         }
         return rsp;
      });
   }

   private <T> CompletionStage<RestResponse> executeCounterCAS(RestRequest request, CASInvocation<StrongCounter, Long, Long, CompletableFuture<T>> invocation) {
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
      String counterName = request.variables().get("counterName");
      boolean pretty = isPretty(request);

      Long expect = checkForNumericParam("expect", request, responseBuilder);
      if (expect == null) return completedFuture(responseBuilder.build());

      Long update = checkForNumericParam("update", request, responseBuilder);
      if (update == null) return completedFuture(responseBuilder.build());

      CompletionStage<StrongCounter> stage = invocationHelper.getCounterManager().getStrongCounterAsync(counterName);
      return CompletionStages.handleAndCompose(stage, (counter, throwable) -> {
         if (throwable != null) {
            return handleThrowable(request, throwable);
         }
         return invocation.apply(counter, expect, update).thenCompose(value -> asJsonResponseFuture(invocationHelper.newResponse(request), Json.make(value), pretty));
      });
   }

   @FunctionalInterface
   interface CASInvocation<C, A, B, R> {
      R apply(C t, A u, B v);
   }

   private Long checkForNumericParam(String name, RestRequest request, NettyRestResponse.Builder builder) {
      List<String> params = request.parameters().get(name);
      if (params == null || params.size() != 1) {
         throw Log.REST.missingArgument(name);
      } else {
         String value = params.iterator().next();
         try {
            return Long.valueOf(value);
         } catch (NumberFormatException e) {
            throw Log.REST.illegalArgument(name, value);
         }
      }
   }

   private MediaType negotiateMediaType(String accept) {
      return MediaType.parseList(accept).filter(t ->
            t.match(MediaType.TEXT_PLAIN) // TODO: add more types in ISPN-10211
      ).findFirst().orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
   }

   private CompletionStage<RestResponse> handleThrowable(RestRequest request, Throwable throwable) {
      Throwable cause = extractException(throwable);
      if (cause instanceof CounterNotFoundException) {
         return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
      }
      return CompletableFuture.failedFuture(cause);
   }
}
