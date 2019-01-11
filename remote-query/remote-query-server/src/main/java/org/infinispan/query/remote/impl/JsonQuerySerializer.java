package org.infinispan.query.remote.impl;

import static java.util.stream.Collectors.toList;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.query.remote.impl.RemoteQueryManager.QUERY_REQUEST_TYPE;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.query.remote.client.impl.QueryRequest;
import org.infinispan.query.remote.json.Hit;
import org.infinispan.query.remote.json.JsonQueryResponse;
import org.infinispan.query.remote.json.JsonQueryResult;
import org.infinispan.query.remote.json.ProjectedJsonResult;

/**
 * Handles serialization of Query response in JSON format.
 *
 * @since 9.4
 */
class JsonQuerySerializer implements QuerySerializer<JsonQueryResponse> {

   private final MediaType storageMediaTye;
   private final Transcoder transcoderFromStorage;
   private final Transcoder transcoderToObject;

   JsonQuerySerializer(MediaType storageMediaTye, Transcoder transcoderFromStorage, Transcoder transcoderToObject) {
      this.storageMediaTye = storageMediaTye;
      this.transcoderFromStorage = transcoderFromStorage;
      this.transcoderToObject = transcoderToObject;
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType mediaType) {
      return (QueryRequest) transcoderToObject.transcode(queryRequest, mediaType, QUERY_REQUEST_TYPE);
   }

   @Override
   public JsonQueryResponse createQueryResponse(RemoteQueryResult remoteQueryResult) {
      int totalResults = remoteQueryResult.getTotalResults();
      String[] projections = remoteQueryResult.getProjections();
      JsonQueryResponse response;
      if (projections == null) {
         List<Object> results = remoteQueryResult.getResults().stream()
               .map(o -> transcoderFromStorage.transcode(o, storageMediaTye, APPLICATION_JSON))
               .collect(toList());
         List<Hit> hits = results.stream().map(Hit::new).collect(Collectors.toList());
         response = new JsonQueryResult(hits, totalResults);
      } else {
         response = new ProjectedJsonResult(totalResults, projections, remoteQueryResult.getResults());
      }
      return response;
   }

   @Override
   public byte[] encodeQueryResponse(Object queryResponse, MediaType destinationType) {
      return ((JsonQueryResponse) queryResponse).asBytes();
   }

}
