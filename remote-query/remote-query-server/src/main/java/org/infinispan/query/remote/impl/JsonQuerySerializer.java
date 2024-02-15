package org.infinispan.query.remote.impl;

import static java.util.stream.Collectors.toList;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.projection.ScorePropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.projection.VersionPropertyPath;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.query.remote.client.impl.QueryRequest;
import org.infinispan.query.remote.json.EntityProjection;
import org.infinispan.query.remote.json.Hit;
import org.infinispan.query.remote.json.JsonProjection;
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
         List<JsonProjection> hits = new ArrayList<>(projections.length);
         for (Object v : remoteQueryResult.getResults()) {
            Object[] result = (Object[]) v;
            Map<String, Object> p = new HashMap<>();
            for (int i = 0; i < projections.length; i++) {
               String projectionKey = projections[i];
               Object value = result[i];
               if (CacheValuePropertyPath.VALUE_PROPERTY_NAME.equals(projectionKey)) {
                  value = new EntityProjection(transcoderFromStorage.transcode(((WrappedMessage) value).getValue(),
                        storageMediaTye, APPLICATION_JSON));
                  p.put(JsonQueryResponse.ENTITY_PROJECTION_KEY, value);
               } else if (ScorePropertyPath.SCORE_PROPERTY_NAME.equals(projectionKey)) {
                  p.put(JsonQueryResponse.SCORE_PROJECTION_KEY, value);
               } else if (VersionPropertyPath.VERSION_PROPERTY_NAME.equals(projectionKey)) {
                  p.put(JsonQueryResponse.VERSION_PROJECTION_KEY, value);
               } else {
                  p.put(projectionKey, value);
               }
            }
            hits.add(new JsonProjection(p));
         }
         response = new ProjectedJsonResult(hitCount, hitCountExact, hits);
      }
      return response;
   }

   @Override
   public byte[] encodeQueryResponse(Object queryResponse, MediaType destinationType) {
      return ((JsonQueryResponse) queryResponse).toJson().toString().getBytes(destinationType.getCharset());
   }
}
