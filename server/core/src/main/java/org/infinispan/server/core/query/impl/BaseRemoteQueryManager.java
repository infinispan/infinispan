package org.infinispan.server.core.query.impl;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.query.BaseQuery;
import org.infinispan.encoding.DataConversion;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.remote.client.impl.QueryRequest;
import org.infinispan.server.core.query.impl.logging.Log;

/**
 * @since 9.4
 */
@Scope(Scopes.NAMED_CACHE)
abstract class BaseRemoteQueryManager implements RemoteQueryManager {

   private static final Log log = Log.getLog(BaseRemoteQueryManager.class);

   final AdvancedCache<?, ?> cache;
   private final QuerySerializers querySerializers;
   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;
   private final boolean cacheQueryable;
   private final MediaType storageType;
   private final boolean unknownMediaType;

   @Inject protected EncoderRegistry encoderRegistry;
   @Inject protected LocalQueryStatistics queryStatistics;

   BaseRemoteQueryManager(AdvancedCache<?, ?> cache, QuerySerializers querySerializers, ComponentRegistry cr) {
      this.cache = cache;
      this.querySerializers = querySerializers;
      this.keyDataConversion = cache.getKeyDataConversion();
      this.valueDataConversion = cache.getValueDataConversion();
      StorageConfigurationManager storageConfigurationManager = cr.getComponent(StorageConfigurationManager.class);
      this.storageType = storageConfigurationManager.getValueStorageMediaType();
      this.cacheQueryable = storageConfigurationManager.isQueryable();
      this.unknownMediaType = storageType.match(MediaType.APPLICATION_UNKNOWN);
   }

   @Override
   public byte[] executeDeleteByQuery(String queryString, Map<String, Object> namedParametersMap,
                                      AdvancedCache<?, ?> cache, MediaType outputFormat, boolean isLocal) {
      QueryResultWithProjection resultWithProjection =
            localQuery(queryString, namedParametersMap, null, null, null, cache, isLocal);
      QueryResult<Object> queryResult = resultWithProjection.queryResult;
      String[] projection = resultWithProjection.projection;

      QuerySerializer<?> querySerializer = querySerializers.getSerializer(outputFormat);
      RemoteQueryResult remoteQueryResult = new RemoteQueryResult(projection, queryResult.count().value(),
            queryResult.count().exact(), queryResult.list());
      Object response = querySerializer.createQueryResponse(remoteQueryResult);
      return querySerializer.encodeQueryResponse(response, outputFormat);
   }

   @Override
   public byte[] executeQuery(String queryString, Map<String, Object> namedParametersMap, Number offset, Number maxResults,
                              Integer hitCountAccuracy, AdvancedCache<?, ?> cache, MediaType outputFormat, boolean isLocal) {
      QueryResultWithProjection resultWithProjection =
              localQuery(queryString, namedParametersMap, offset, maxResults, hitCountAccuracy, cache, isLocal);
      QueryResult<Object> queryResult = resultWithProjection.queryResult;
      String[] projection = resultWithProjection.projection;

      QuerySerializer<?> querySerializer = querySerializers.getSerializer(outputFormat);
      RemoteQueryResult remoteQueryResult = new RemoteQueryResult(projection, queryResult.count().value(),
              queryResult.count().exact(), queryResult.list());
      Object response = querySerializer.createQueryResponse(remoteQueryResult);
      return querySerializer.encodeQueryResponse(response, outputFormat);
   }

   @Override
   public QueryResultWithProjection localQuery(String queryString, Map<String, Object> namedParametersMap, Number offset, Number maxResults,
                                               Integer hitCountAccuracy, AdvancedCache<?, ?> cache, boolean isLocal) {
      if (unknownMediaType) {
         log.warnNoMediaType(cache.getName());
      } else if (!cacheQueryable) {
         throw log.cacheNotQueryable(cache.getName(), storageType.getTypeSubtype());
      }
      Query<Object> query = getQueryEngine(cache).makeQuery(queryString, namedParametersMap, offset, maxResults,
            hitCountAccuracy, isLocal);
      QueryResult<Object> execute = query.execute();
      return new QueryResultWithProjection(execute, ((BaseQuery)query).getProjection());
   }

   public Object convertKey(Object key, MediaType destinationFormat) {
      return encoderRegistry.convert(key, keyDataConversion.getStorageMediaType(), destinationFormat);
   }

   public Object convertValue(Object value, MediaType destinationFormat) {
      return encoderRegistry.convert(value, valueDataConversion.getStorageMediaType(), destinationFormat);
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType requestType) {
      return querySerializers.getSerializer(requestType).decodeQueryRequest(queryRequest, requestType);
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   public record QueryResultWithProjection(QueryResult<Object> queryResult, String[] projection) {
   }
}
