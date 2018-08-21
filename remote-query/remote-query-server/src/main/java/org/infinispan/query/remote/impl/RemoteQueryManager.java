package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.encoding.DataConversion;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.client.QueryRequest;

/**
 * Manages components used during indexed and index-less query.
 *
 * @since 9.2
 */
public interface RemoteQueryManager {

   MediaType PROTOSTREAM_UNWRAPPED = APPLICATION_PROTOSTREAM.withParameter("wrapped", "false");
   MediaType QUERY_REQUEST_TYPE = APPLICATION_OBJECT.withClassType(QueryRequest.class);

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
    * @param filterResult the {@link FilterResult} from filtering and continuous query operations.
    * @return Encoded FilterResult to send to the remote client.
    */
   Object encodeFilterResult(Object filterResult);

   default boolean isQueryEnabled(AdvancedCache<byte[], byte[]> cache) {
      return getQueryEngine(cache) != null;
   }

   Object convertKey(Object key, MediaType destinationFormat);

   Object convertValue(Object value, MediaType destinationFormat);

   DataConversion getKeyDataConversion();

   DataConversion getValueDataConversion();

   byte[] executeQuery(String q, Map<String, Object> namedParametersMap, Integer offset, Integer maxResults,
                       IndexedQueryMode queryMode, AdvancedCache cache, MediaType outputFormat);

}
