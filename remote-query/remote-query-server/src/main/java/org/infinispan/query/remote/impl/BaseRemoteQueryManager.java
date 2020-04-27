package org.infinispan.query.remote.impl;

import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.impl.QueryRequest;

/**
 * @since 9.4
 */
@Scope(Scopes.NAMED_CACHE)
abstract class BaseRemoteQueryManager implements RemoteQueryManager {

   final AdvancedCache<?, ?> cache;
   private final QuerySerializers querySerializers;
   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   @Inject protected EncoderRegistry encoderRegistry;

   BaseRemoteQueryManager(AdvancedCache<?, ?> cache, QuerySerializers querySerializers) {
      this.cache = cache;
      this.querySerializers = querySerializers;
      this.keyDataConversion = cache.getKeyDataConversion();
      this.valueDataConversion = cache.getValueDataConversion();
   }

   @Override
   public byte[] executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset, Integer maxResults,
                              IndexedQueryMode queryMode, AdvancedCache<?, ?> cache, MediaType outputFormat) {
      QuerySerializer<?> querySerializer = querySerializers.getSerializer(outputFormat);
      Query query = getQueryEngine(cache).makeQuery(queryString, namedParametersMap, offset, maxResults, queryMode);
      List<Object> results = query.list();
      int totalResults = query.getResultSize();
      String[] projection = query.getProjection();
      RemoteQueryResult remoteQueryResult = new RemoteQueryResult(projection, totalResults, results);
      Object response = querySerializer.createQueryResponse(remoteQueryResult);
      return querySerializer.encodeQueryResponse(response, outputFormat);
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
}
