package org.infinispan.client.hotrod.impl.operations;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.query.RemoteQuery;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.remote.client.QueryRequest;
import org.infinispan.query.remote.client.QueryResponse;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

/**
 * @since 9.4
 */
enum QuerySerializer {

   JSON(APPLICATION_JSON) {
      private final Gson mapper = new GsonBuilder().disableHtmlEscaping().create();

      @Override
      byte[] serializeQueryRequest(RemoteQuery remoteQuery, QueryRequest queryRequest) {
         JsonObject object = mapper.toJsonTree(queryRequest).getAsJsonObject();
         return object.toString().getBytes(UTF_8);
      }

      @Override
      Object readQueryResponse(Marshaller marshaller, RemoteQuery remoteQuery, byte[] bytesResponse) {
         try (JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytesResponse)))) {
            return mapper.<JsonObject>fromJson(reader, JsonObject.class);
         } catch (IOException e) {
            throw new HotRodClientException(e);
         }
      }
   },

   DEFAULT(MATCH_ALL) {
      @Override
      byte[] serializeQueryRequest(RemoteQuery remoteQuery, QueryRequest queryRequest) {
         final SerializationContext serCtx = remoteQuery.getSerializationContext();
         Marshaller marshaller;
         if (serCtx != null) {
            try {
               return ProtobufUtil.toByteArray(serCtx, queryRequest);
            } catch (IOException e) {
               throw new HotRodClientException(e);
            }
         } else {
            marshaller = remoteQuery.getCache().getRemoteCacheManager().getMarshaller();
            try {
               return marshaller.objectToByteBuffer(queryRequest);
            } catch (IOException e) {
               throw new HotRodClientException(e);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new HotRodClientException(e);
            }
         }
      }

      @Override
      Object readQueryResponse(Marshaller marshaller, RemoteQuery remoteQuery, byte[] bytesResponse) {
         SerializationContext serCtx = remoteQuery.getSerializationContext();
         if (serCtx != null) {
            try {
               return ProtobufUtil.fromByteArray(serCtx, bytesResponse, QueryResponse.class);
            } catch (IOException e) {
               throw new HotRodClientException(e);
            }
         } else {
            try {
               return marshaller.objectFromByteBuffer(bytesResponse);
            } catch (IOException | ClassNotFoundException e) {
               throw new HotRodClientException(e);
            }
         }
      }
   };

   private final MediaType mediaType;

   QuerySerializer(MediaType mediaType) {
      this.mediaType = mediaType;
   }

   @Override
   public String toString() {
      return mediaType.getTypeSubtype();
   }

   static QuerySerializer findByMediaType(MediaType mediaType) {
      return mediaType != null && mediaType.match(APPLICATION_JSON) ? JSON : DEFAULT;
   }

   abstract byte[] serializeQueryRequest(RemoteQuery remoteQuery, QueryRequest queryRequest);

   abstract Object readQueryResponse(Marshaller marshaller, RemoteQuery remoteQuery, byte[] bytesResponse);
}
