package org.infinispan.query.remote.impl;

import static java.util.stream.Collectors.toList;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.encoding.DataConversion;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

/**
 * Manages components used during indexed and index-less query.
 *
 * @since 9.2
 */
public interface RemoteQueryManager {

   MediaType PROTOSTREAM_UNWRAPPED = APPLICATION_PROTOSTREAM.withParameter("wrapped", "false");
   MediaType QUERY_REQUEST_TYPE = APPLICATION_OBJECT.withParameter("type", QueryRequest.class.getName());

   /**
    * @return {@link Matcher} to be used during non-indexed query and filter operations.
    */
   Class<? extends Matcher> getMatcherClass(MediaType mediaType);

   /**
    * @return {@link QueryEngine}
    */
   BaseRemoteQueryEngine getQueryEngine(AdvancedCache<?, ?> cache);

   /**
    * @param queryRequest serialized {@link QueryRequest} provided by the remote client.
    * @return decoded {@link QueryRequest}.
    */
   QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType requestType);

   /**
    * @param queryResponse {@link QueryResponse} carrying the result of the remote query.
    * @return encoded response to send back to the remote client.
    */
   byte[] encodeQueryResponse(QueryResponse queryResponse, MediaType destinationType);

   /**
    * @param filterResult the {@link FilterResult} from filtering and continuous query operations.
    * @return Encoded FilterResult to send to the remote client.
    */
   Object encodeFilterResult(Object filterResult);

   default Object convertKey(Object key, MediaType destinationFormat) {
      DataConversion keyDataConversion = getKeyDataConversion();
      MediaType storageMediaType = keyDataConversion.getStorageMediaType();
      return keyDataConversion.convert(key, storageMediaType, destinationFormat);
   }

   default Object convertValue(Object value, MediaType destinationFormat) {
      DataConversion valueDataConversion = getValueDataConversion();
      MediaType storageMediaType = valueDataConversion.getStorageMediaType();
      return valueDataConversion.convert(value, storageMediaType, destinationFormat);
   }

   DataConversion getKeyDataConversion();

   DataConversion getValueDataConversion();

   default List<Object> encodeQueryResults(List<Object> results) {
      DataConversion valueDataConversion = getValueDataConversion();
      return results.stream()
            .map(o -> valueDataConversion.convert(o, valueDataConversion.getStorageMediaType(), APPLICATION_JSON))
            .collect(toList());
   }

   default RemoteQueryResult executeQuery(String q, Integer offset, Integer maxResults, IndexedQueryMode queryMode, AdvancedCache cache) {
      Query query = getQueryEngine(cache).makeQuery(q, null, offset, maxResults, queryMode);
      List<Object> results = query.list();
      String[] projection = query.getProjection();
      int totalResults = query.getResultSize();
      if (projection == null) {
         return new RemoteQueryResult(null, totalResults, encodeQueryResults(results));
      } else {
         return new RemoteQueryResult(projection, totalResults, results);
      }
   }
}
