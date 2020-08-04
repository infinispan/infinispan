package org.infinispan.query.remote.impl;

import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.encoding.DataConversion;
import org.infinispan.encoding.impl.StorageConfigurationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.remote.client.impl.QueryRequest;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * @since 9.4
 */
@Scope(Scopes.NAMED_CACHE)
abstract class BaseRemoteQueryManager implements RemoteQueryManager {

   private static final Log log = LogFactory.getLog(BaseRemoteQueryManager.class, Log.class);

   final AdvancedCache<?, ?> cache;
   private final QuerySerializers querySerializers;
   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;
   private final boolean cacheQueryable;
   private final MediaType storageType;
   private final boolean unknownMediaType;

   @Inject protected EncoderRegistry encoderRegistry;

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
   public byte[] executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset, Integer maxResults,
                              AdvancedCache<?, ?> cache, MediaType outputFormat) {
      if (unknownMediaType) {
         log.warnNoMediaType(cache.getName());
      } else if (!cacheQueryable) {
         throw log.cacheNotQueryable(cache.getName(), storageType.getTypeSubtype());
      }

      QuerySerializer<?> querySerializer = querySerializers.getSerializer(outputFormat);
      BaseQuery<Object> query = getQueryEngine(cache).makeQuery(queryString, namedParametersMap, offset, maxResults);
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
