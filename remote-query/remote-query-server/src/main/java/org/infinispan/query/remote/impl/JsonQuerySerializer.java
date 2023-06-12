package org.infinispan.query.remote.impl;

import static java.util.stream.Collectors.toList;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.internal.Json;
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

   JsonQuerySerializer(MediaType storageMediaTye, Transcoder transcoderFromStorage) {
      this.storageMediaTye = storageMediaTye;
      this.transcoderFromStorage = transcoderFromStorage;
   }

   @Override
   public QueryRequest decodeQueryRequest(byte[] queryRequest, MediaType mediaType) {
      return QueryRequest.fromJson(Json.read(new String(queryRequest, mediaType.getCharset())));
   }

   @Override
   public JsonQueryResponse createQueryResponse(RemoteQueryResult remoteQueryResult) {
      int hitCount = remoteQueryResult.hitCount();
      boolean hitCountExact = remoteQueryResult.hitCountExact();
      String[] projections = remoteQueryResult.getProjections();
      JsonQueryResponse response;
      if (projections == null) {
         List<Object> results = remoteQueryResult.getResults().stream()
               .map(o -> transcoderFromStorage.transcode(o, storageMediaTye, APPLICATION_JSON))
               .collect(toList());
         List<Hit> hits = results.stream().map(Hit::new).collect(Collectors.toList());
         response = new JsonQueryResult(hits, hitCount, hitCountExact);
      } else {
         response = new ProjectedJsonResult(hitCount, hitCountExact, projections, remoteQueryResult.getResults());
      }
      return response;
   }

   @Override
   public byte[] encodeQueryResponse(Object queryResponse, MediaType destinationType) {
      return ((JsonQueryResponse) queryResponse).toJson().toString().getBytes(destinationType.getCharset());
   }

}
