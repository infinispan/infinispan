package org.infinispan.query.remote.impl;

import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.remote.client.QueryRequest;

/**
 * @since 9.4
 */
abstract class BaseRemoteQueryManager implements RemoteQueryManager {

   final AdvancedCache<?, ?> cache;
   final QuerySerializers querySerializers;
   final DataConversion keyDataConversion;
   final DataConversion valueDataConversion;

   BaseRemoteQueryManager(ComponentRegistry cr, QuerySerializers querySerializers) {
      this.cache = cr.getComponent(Cache.class).getAdvancedCache();
      this.querySerializers = querySerializers;
      this.keyDataConversion = cache.getKeyDataConversion();
      this.valueDataConversion = cache.getValueDataConversion();
   }

   public byte[] executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset, Integer maxResults,
                              IndexedQueryMode queryMode, AdvancedCache cache, MediaType outputFormat) {
      QuerySerializer querySerializer = querySerializers.getSerializer(outputFormat);
      Query query = getQueryEngine(cache).makeQuery(queryString, namedParametersMap, offset, maxResults, queryMode);
      List<Object> results = query.list();
      int totalResults = query.getResultSize();
      String[] projection = query.getProjection();
      RemoteQueryResult remoteQueryResult = new RemoteQueryResult(projection, totalResults, results);
      Object response = querySerializer.createQueryResponse(remoteQueryResult);
      return querySerializer.encodeQueryResponse(response, outputFormat);
   }

   public Object convertKey(Object key, MediaType destinationFormat) {
      DataConversion keyDataConversion = getKeyDataConversion();
      MediaType storageMediaType = keyDataConversion.getStorageMediaType();
      return keyDataConversion.convert(key, storageMediaType, destinationFormat);
   }

   public Object convertValue(Object value, MediaType destinationFormat) {
      DataConversion valueDataConversion = getValueDataConversion();
      MediaType storageMediaType = valueDataConversion.getStorageMediaType();
      return valueDataConversion.convert(value, storageMediaType, destinationFormat);
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
