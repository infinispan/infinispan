package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;
import static org.infinispan.query.remote.impl.RemoteQueryManager.PROTOSTREAM_UNWRAPPED;
import static org.infinispan.query.remote.impl.RemoteQueryManager.QUERY_REQUEST_TYPE;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.remote.client.impl.QueryRequest;
import org.infinispan.query.remote.client.impl.QueryResponse;

/**
 * Handles serialization of Query results in marshalled {@link QueryResponse} format.
 *
 * @since 9.4
 */
class DefaultQuerySerializer implements QuerySerializer<QueryResponse> {

   private final EncoderRegistry encoderRegistry;

   DefaultQuerySerializer(EncoderRegistry encoderRegistry) {
      this.encoderRegistry = encoderRegistry;
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType mediaType) {
      // QueryRequests are sent in 'unwrapped' protobuf format
      if (mediaType.match(APPLICATION_PROTOSTREAM)) mediaType = PROTOSTREAM_UNWRAPPED;

      Transcoder transcoder = encoderRegistry.getTranscoder(APPLICATION_OBJECT, mediaType);
      return (QueryRequest) transcoder.transcode(queryRequest, mediaType, QUERY_REQUEST_TYPE);
   }

   @Override
   public QueryResponse createQueryResponse(RemoteQueryResult remoteQueryResult) {
      List<?> list = remoteQueryResult.getResults();
      int numResults = list.size();
      String[] projection = remoteQueryResult.getProjections();
      int projSize = projection != null ? projection.length : 0;
      List<WrappedMessage> results = new ArrayList<>(projSize == 0 ? numResults : numResults * projSize);

      for (Object o : list) {
         if (projSize == 0) {
            results.add(new WrappedMessage(o));
         } else {
            Object[] row = (Object[]) o;
            for (int i = 0; i < projSize; i++) {
               results.add(new WrappedMessage(row[i]));
            }
         }
      }
      QueryResponse response = new QueryResponse();
      response.hitCount(remoteQueryResult.hitCount());
      response.hitCountExact(remoteQueryResult.hitCountExact());
      response.setNumResults(numResults);
      response.setProjectionSize(projSize);
      response.setResults(results);
      return response;
   }


   public byte[] encodeQueryResponse(Object queryResponse, MediaType destinationType) {
      MediaType destination = destinationType;
      // QueryResponses are sent in 'unwrapped' protobuf format
      if (destinationType.match(APPLICATION_PROTOSTREAM)) destination = PROTOSTREAM_UNWRAPPED;
      Transcoder transcoder = encoderRegistry.getTranscoder(APPLICATION_OBJECT, destinationType);
      return (byte[]) transcoder.transcode(queryResponse, APPLICATION_OBJECT, destination);
   }

}
