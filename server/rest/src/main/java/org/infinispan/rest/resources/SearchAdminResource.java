package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.rest.framework.Method.GET;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.InfinispanQueryStatisticsInfo;
import org.infinispan.query.impl.massindex.MassIndexerAlreadyStartedException;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.1
 */
public class SearchAdminResource implements ResourceHandler {

   private final static Log LOG = LogFactory.getLog(SearchAdminResource.class, Log.class);

   private final InvocationHelper invocationHelper;
   private final boolean globalStatsEnabled;

   public SearchAdminResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      this.globalStatsEnabled = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().enabled();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/indexes").withAction("mass-index").handleWith(this::massIndex)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/indexes").withAction("clear").handleWith(this::clearIndexes)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/indexes/stats").handleWith(this::indexStats)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/query/stats").handleWith(this::queryStats)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/query/stats").withAction("clear").handleWith(this::clearStats)
            .create();
   }

   private CompletionStage<RestResponse> massIndex(RestRequest request) {
      return runMassIndexer(request, MassIndexer::startAsync, true);
   }

   private CompletionStage<RestResponse> clearIndexes(RestRequest request) {
      return runMassIndexer(request, MassIndexer::purge, false);
   }

   private CompletionStage<RestResponse> indexStats(RestRequest request) {
      return showStats(request, InfinispanQueryStatisticsInfo::getIndexStatistics);
   }

   private CompletionStage<RestResponse> queryStats(RestRequest request) {
      return showStats(request, InfinispanQueryStatisticsInfo::getQueryStatistics);
   }

   private CompletionStage<RestResponse> clearStats(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      InfinispanQueryStatisticsInfo queryStatistics = lookupQueryStatistics(request, responseBuilder);

      if (queryStatistics == null) return completedFuture(responseBuilder.build());

      queryStatistics.clear();
      return completedFuture(responseBuilder.build());
   }

   private CompletionStage<RestResponse> runMassIndexer(RestRequest request,
                                                        Function<MassIndexer, CompletableFuture<Void>> op,
                                                        boolean supportAsync) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      List<String> mode = request.parameters().get("mode");

      boolean asyncParams = mode != null && !mode.isEmpty() && mode.iterator().next().equalsIgnoreCase("async");
      boolean async = asyncParams && supportAsync;

      AdvancedCache<?, ?> cache = lookupIndexedCache(request, responseBuilder);
      if (responseBuilder.getStatus() != OK.code()) {
         return completedFuture(responseBuilder.build());
      }

      MassIndexer massIndexer = ComponentRegistryUtils.getMassIndexer(cache);

      if (async) {
         try {
            LOG.asyncMassIndexerStarted();
            op.apply(massIndexer).whenComplete((v,e) -> {
               if(e == null) {
                  LOG.asyncMassIndexerSuccess();
               } else {
                  LOG.errorExecutingMassIndexer(e.getCause());
               }
            });
         } catch (Exception e) {
            responseBuilder.status(INTERNAL_SERVER_ERROR.code()).entity("Error executing the MassIndexer " + e.getCause()).build();
         }
         return CompletableFuture.completedFuture(responseBuilder.build());
      }

      return op.apply(massIndexer).exceptionally(e -> {
         if (e instanceof MassIndexerAlreadyStartedException) {
            responseBuilder.status(BAD_REQUEST.code()).entity("MassIndexer Already Started").build();
         } else {
            responseBuilder.status(INTERNAL_SERVER_ERROR.code()).entity("Error executing the MassIndexer " + e.getCause()).build();
         }
         return null;
      }).thenApply(v -> responseBuilder.build());
   }


   private CompletionStage<RestResponse> showStats(RestRequest request, Function<InfinispanQueryStatisticsInfo, Object> statExtractor) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      InfinispanQueryStatisticsInfo searchStats = lookupQueryStatistics(request, responseBuilder);

      if (searchStats == null) return completedFuture(responseBuilder.build());

      try {
         byte[] bytes = invocationHelper.getMapper().writeValueAsBytes(statExtractor.apply(searchStats));
         responseBuilder.contentType(APPLICATION_JSON).entity(bytes).status(OK);
      } catch (JsonProcessingException e) {
         responseBuilder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
      return completedFuture(responseBuilder.build());
   }

   private AdvancedCache<?, ?> lookupIndexedCache(RestRequest request, NettyRestResponse.Builder builder) {
      String cacheName = request.variables().get("cacheName");
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         builder.status(HttpResponseStatus.NOT_FOUND.code()).build();
         return null;
      }
      Configuration cacheConfiguration = cache.getCacheConfiguration();
      if (!cacheConfiguration.indexing().index().isEnabled()) {
         builder.entity("cache is not indexed").status(BAD_REQUEST.code()).build();
      }
      return cache;
   }

   private AdvancedCache<?, ?> lookupCacheWithStats(RestRequest request, NettyRestResponse.Builder builder) {
      AdvancedCache<?, ?> cache = lookupIndexedCache(request, builder);

      if (!globalStatsEnabled) builder.entity("statistics not enabled").status(BAD_REQUEST.code()).build();

      if (cache != null) {
         Configuration cacheConfiguration = cache.getCacheConfiguration();
         if (!cacheConfiguration.jmxStatistics().enabled()) {
            builder.entity("statistics not enabled").status(BAD_REQUEST.code());
         }
      }
      return cache;
   }

   private InfinispanQueryStatisticsInfo lookupQueryStatistics(RestRequest request, NettyRestResponse.Builder builder) {
      AdvancedCache<?, ?> cache = lookupCacheWithStats(request, builder);
      if (builder.getStatus() != HttpResponseStatus.OK.code()) {
         return null;
      }
      return ComponentRegistryUtils.getQueryStatistics(cache);
   }
}
