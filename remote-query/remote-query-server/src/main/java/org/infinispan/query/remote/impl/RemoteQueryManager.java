package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.encoding.ProtostreamTranscoder.WRAPPED_PARAM;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.encoding.DataConversion;
import org.infinispan.objectfilter.Matcher;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.client.impl.QueryRequest;

/**
 * Manages components used during indexed and index-less query.
 *
 * @since 9.2
 */
public interface RemoteQueryManager {

   MediaType PROTOSTREAM_UNWRAPPED = APPLICATION_PROTOSTREAM.withParameter(WRAPPED_PARAM, "false");
   MediaType QUERY_REQUEST_TYPE = APPLICATION_OBJECT.withClassType(QueryRequest.class);

   /**
    * @return {@link Matcher} to be used during non-indexed query and filter operations.
    */
   Class<? extends Matcher> getMatcherClass(MediaType mediaType);

   /**
    * @return {@link ObjectRemoteQueryEngine}
    */
   ObjectRemoteQueryEngine getQueryEngine(AdvancedCache<?, ?> cache);

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

   Object convertKey(Object key, MediaType destinationFormat);

   Object convertValue(Object value, MediaType destinationFormat);

   DataConversion getKeyDataConversion();

   DataConversion getValueDataConversion();

   byte[] executeQuery(String queryString, Map<String, Object> namedParametersMap, Integer offset, Integer maxResults,
                       AdvancedCache<?, ?> cache, MediaType outputFormat);
}
