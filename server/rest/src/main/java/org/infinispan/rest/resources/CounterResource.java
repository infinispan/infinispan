package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpHeaderNames.CACHE_CONTROL;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
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
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.ConvertUtil;
import org.infinispan.counter.configuration.CounterConfigurationSerializer;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
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

      if (contents == null || contents.length() == 0) {
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
         return CompletableFuture.runAsync(() -> counterManager.undefineCounter(counterName), invocationHelper.getExecutor())
               .thenApply(ignore -> new NettyRestResponse.Builder().status(NO_CONTENT).build());
      });
   }

   private CompletionStage<RestResponse> getConfig(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");
      return invocationHelper.getCounterManager().getConfigurationAsync(counterName).thenApply(cfg -> {
         if (cfg == null) return responseBuilder.status(NOT_FOUND).build();

         AbstractCounterConfiguration parsedConfig = ConvertUtil.configToParsedConfig(counterName, cfg);
         CounterConfigurationSerializer ccs = new CounterConfigurationSerializer();
         StringBuilderWriter sw = new StringBuilderWriter();
         try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).build()) {
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
      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return notFoundResponseFuture();

         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder()
               .contentType(contentType)
               .header(CACHE_CONTROL.toString(), CacheControl.noCache());

         CompletionStage<Long> response;
         if (configuration.type() == CounterType.WEAK) {
            response = getWeakCounter(counterName, counterManager)
                  .thenApply(WeakCounter::getValue);
         } else {
            response = getStrongCounter(counterName, counterManager)
                  .thenCompose(StrongCounter::getValue);
         }

         return response.thenApply(v -> responseBuilder.entity(Long.toString(v)).build());
      });
   }

   private CompletionStage<RestResponse> resetCounter(RestRequest request) throws RestResponseException {
      String counterName = request.variables().get("counterName");

      EmbeddedCounterManager counterManager = invocationHelper.getCounterManager();
      return counterManager.getConfigurationAsync(counterName).thenCompose(configuration -> {
         if (configuration == null) return notFoundResponseFuture();
         CompletionStage<Void> result = configuration.type() == CounterType.WEAK ?
               counterManager.getWeakCounter(counterName).reset() :
               counterManager.getStrongCounter(counterName).reset();

         return result.thenApply(v -> new NettyRestResponse.Builder().status(NO_CONTENT).build());
      });
   }

   private CompletionStage<RestResponse> getCounterNames(RestRequest request) throws RestResponseException {
      return asJsonResponseFuture(Json.make(invocationHelper.getCounterManager().getCounterNames()));
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

      CompletableFuture<CounterConfiguration> counterConfigAsync = invocationHelper.getCounterManager().getConfigurationAsync(counterName);

      return counterConfigAsync.thenCompose(configuration -> {
         if (configuration == null) return notFoundResponseFuture();
         CounterType type = configuration.type();

         if (type == CounterType.WEAK) return executeWeakCounterOp(counterName, weakOp);

         return executeStrongCounterOp(counterName, strongOp);
      });
   }

   private <T> CompletionStage<RestResponse> executeCounterCAS(RestRequest request, CASInvocation<StrongCounter, Long, Long, CompletableFuture<T>> invocation) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      String counterName = request.variables().get("counterName");

      Long expect = checkForNumericParam("expect", request, responseBuilder);
      if (expect == null) return completedFuture(responseBuilder.build());

      Long update = checkForNumericParam("update", request, responseBuilder);
      if (update == null) return completedFuture(responseBuilder.build());

      CompletionStage<StrongCounter> strongCounter = getStrongCounter(counterName, invocationHelper.getCounterManager());
      return strongCounter.thenCompose(counter -> {
         if (counter == null) {
            responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity(String.format("Strong counter '%s' not found", counterName));
            return completedFuture(responseBuilder.build());
         }
         CompletableFuture<T> opResult = invocation.apply(counter, expect, update);

         return opResult.thenCompose(value -> asJsonResponseFuture(Json.make(value)));
      });
   }

   @FunctionalInterface
   interface CASInvocation<C, A, B, R> {
      R apply(C t, A u, B v);
   }

   private CompletionStage<RestResponse> executeStrongCounterOp(String counterName, Function<StrongCounter, CompletableFuture<Long>> op) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      CompletionStage<StrongCounter> strongCounter = getStrongCounter(counterName, invocationHelper.getCounterManager());
      return strongCounter.thenCompose(counter -> {
         if (counter == null) {
            responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity(String.format("Strong counter '%s' not found", counterName));
            return completedFuture(responseBuilder.build());
         }
         return op.apply(counter)
               .thenCompose(value -> asJsonResponseFuture(Json.make(value), responseBuilder));
      });
   }

   private CompletionStage<WeakCounter> getWeakCounter(String name, EmbeddedCounterManager counterManager) {
      WeakCounter weakCounter = counterManager.getCreatedWeakCounter(name);
      if (weakCounter != null) {
         return CompletableFuture.completedFuture(weakCounter);
      }
      return CompletableFuture.supplyAsync(() -> counterManager.getWeakCounter(name), invocationHelper.getExecutor())
            .exceptionally(ignore -> null);
   }

   private CompletionStage<StrongCounter> getStrongCounter(String name, EmbeddedCounterManager counterManager) {
      StrongCounter strongCounter = counterManager.getCreatedStrongCounter(name);
      if (strongCounter != null) {
         return CompletableFuture.completedFuture(strongCounter);
      }
      return CompletableFuture.supplyAsync(() -> counterManager.getStrongCounter(name), invocationHelper.getExecutor())
            .exceptionally(ignore -> null);
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
      responseBuilder.status(NO_CONTENT);

      CompletionStage<WeakCounter> weakCounter = getWeakCounter(counterName, invocationHelper.getCounterManager());
      return weakCounter.thenCompose(counter -> {
         if (counter == null) {
            responseBuilder.status(HttpResponseStatus.BAD_REQUEST).entity(String.format("Weak counter '%s' not found", counterName));
            return completedFuture(responseBuilder.build());
         }
         return op.apply(counter).thenCompose(t -> completedFuture(responseBuilder.build()));
      });
   }

   private MediaType negotiateMediaType(String accept) {
      return MediaType.parseList(accept).filter(t ->
            t.match(MediaType.TEXT_PLAIN) // TODO: add more types in ISPN-10211
      ).findFirst().orElseThrow(() -> Log.REST.unsupportedDataFormat(accept));
   }
}
