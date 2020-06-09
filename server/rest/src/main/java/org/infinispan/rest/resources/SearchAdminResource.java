package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.query.Indexer;
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

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.1
 */
public class SearchAdminResource implements ResourceHandler {

   private final static Log LOG = LogFactory.getLog(SearchAdminResource.class, Log.class);

   private final InvocationHelper invocationHelper;

   public SearchAdminResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/indexes").withAction("mass-index").handleWith(this::reindex)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/indexes").withAction("clear").handleWith(this::clearIndexes)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/indexes/stats").handleWith(this::indexStats)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/query/stats").handleWith(this::queryStats)
            .invocation().methods(GET).path("/v2/caches/{cacheName}/search/query/stats").withAction("clear").handleWith(this::clearStats)
            .create();
   }

   private CompletionStage<RestResponse> reindex(RestRequest request) {
      return runIndexer(request, Indexer::run, true);
   }

   private CompletionStage<RestResponse> clearIndexes(RestRequest request) {
      return runIndexer(request, Indexer::remove, false);
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

   private CompletionStage<RestResponse> runIndexer(RestRequest request,
                                                    Function<Indexer, CompletionStage<Void>> op,
                                                    boolean supportAsync) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      List<String> mode = request.parameters().get("mode");

      boolean asyncParams = mode != null && !mode.isEmpty() && mode.iterator().next().equalsIgnoreCase("async");
      boolean async = asyncParams && supportAsync;

      AdvancedCache<?, ?> cache = lookupIndexedCache(request, responseBuilder);
      if (responseBuilder.getStatus() != OK.code()) {
         return completedFuture(responseBuilder.build());
      }

      Indexer indexer = ComponentRegistryUtils.getIndexer(cache);

      if (async) {
         try {
            LOG.asyncMassIndexerStarted();
            op.apply(indexer).whenComplete((v,e) -> {
               if(e == null) {
                  LOG.asyncMassIndexerSuccess();
               } else {
                  LOG.errorExecutingMassIndexer(e.getCause());
               }
            });
         } catch (Exception e) {
            responseBuilder.status(INTERNAL_SERVER_ERROR.code()).entity("Error executing the MassIndexer " + e.getCause());
         }
         return CompletableFuture.completedFuture(responseBuilder.build());
      }

      return op.apply(indexer).exceptionally(e -> {
         if (e instanceof MassIndexerAlreadyStartedException) {
            responseBuilder.status(BAD_REQUEST.code()).entity("MassIndexer already started");
         } else {
            responseBuilder.status(INTERNAL_SERVER_ERROR.code()).entity("Error executing the MassIndexer " + e.getCause());
         }
         return null;
      }).thenApply(v -> responseBuilder.build());
   }

   private CompletionStage<RestResponse> showStats(RestRequest request, Function<InfinispanQueryStatisticsInfo, Object> statExtractor) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      InfinispanQueryStatisticsInfo searchStats = lookupQueryStatistics(request, responseBuilder);
      if (searchStats == null) return completedFuture(responseBuilder.build());

      return asJsonResponseFuture(statExtractor.apply(searchStats), responseBuilder, invocationHelper);
   }

   private AdvancedCache<?, ?> lookupIndexedCache(RestRequest request, NettyRestResponse.Builder builder) {
      String cacheName = request.variables().get("cacheName");
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      if (cache == null) {
         builder.status(HttpResponseStatus.NOT_FOUND.code());
         return null;
      }
      Configuration cacheConfiguration = cache.getCacheConfiguration();
      if (!cacheConfiguration.indexing().enabled()) {
         builder.entity("cache is not indexed").status(BAD_REQUEST.code()).build();
      }
      return cache;
   }

   private AdvancedCache<?, ?> lookupCacheWithStats(RestRequest request, NettyRestResponse.Builder builder) {
      AdvancedCache<?, ?> cache = lookupIndexedCache(request, builder);
      if (cache != null) {
         Configuration cacheConfiguration = cache.getCacheConfiguration();
         if (!cacheConfiguration.statistics().enabled()) {
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
