package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;

import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.operations.exceptions.NoDataFoundException;
import org.infinispan.rest.operations.exceptions.NoKeyException;
import org.infinispan.util.concurrent.CompletableFutures;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Protobuf schema manipulation Resource
 *
 * @author Katia Aresti
 * @since 11
 */
public class ProtobufResource extends BaseCacheResource implements ResourceHandler {

   private final ObjectMapper objectMapper;

   public ProtobufResource(InvocationHelper invocationHelper) {
      super(invocationHelper);
      objectMapper = invocationHelper.getMapper();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            // Key related operations
            .invocation().methods(GET).path("/v2/schemas").handleWith(this::getSchemasNames)
            .invocation().methods(POST).path("/v2/schemas/{schemaName}").handleWith(r -> createOrReplace(r,true))
            .invocation().methods(PUT).path("/v2/schemas/{schemaName}").handleWith(r -> createOrReplace(r,false))
            .invocation().methods(GET).path("/v2/schemas/{schemaName}").handleWith(this::getSchema)
            .invocation().method(DELETE).path("/v2/schemas/{schemaName}").handleWith(this::deleteSchema)
            .create();
   }

   private CompletionStage<RestResponse> getSchemasNames(RestRequest request) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager()
            .getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);

      return CompletableFuture.supplyAsync(() ->
          Flowable.fromIterable(cache.keySet())
               .filter(key -> !((String)key).endsWith(ProtobufMetadataManager.ERRORS_KEY_SUFFIX))
               .map(key -> {
                  String error = (String) cache
                        .get(key + ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
                  ProtoSchema protoSchema = new ProtoSchema();
                  protoSchema.name = (String) key;
                  if (error != null) {
                     protoSchema.error = createErrorContent(protoSchema.name, error);
                  }
                  return protoSchema;
               })
                .sorted(Comparator.comparing(s -> s.name))
                .collect(Collectors.toList())
                .map(protoSchemas -> asJsonResponse(protoSchemas, invocationHelper))
                .toCompletionStage()
      , invocationHelper.getExecutor())
            .thenCompose(Function.identity());
   }

   private CompletionStage<RestResponse> createOrReplace(RestRequest request, boolean create) {

      String schemaName = checkMandatorySchemaName(request);

      ContentSource contents = request.contents();
      if (contents == null) throw new NoDataFoundException("Schema data not sent in the request");

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager()
            .getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);

      NettyRestResponse.Builder builder = new NettyRestResponse.Builder();

      CompletableFuture<Object> putSchema;
      if (create) {
         putSchema = cache.putIfAbsentAsync(schemaName, contents.asString()).thenApply(result -> {
            if (result == null) {
               builder.status(HttpResponseStatus.CREATED);
            } else {
               builder.status(HttpResponseStatus.CONFLICT);
            }
            return result;
         });
      } else {
         putSchema = cache.putAsync(schemaName, contents.asString()).thenApply(result -> builder.status(HttpResponseStatus.OK));
      }

      return putSchema.thenCompose(r -> {
         if (isOkOrCreated(builder)) {
            return cache.getAsync(schemaName + ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
         } else {
            return CompletableFutures.completedNull();
         }
      }).thenApply(validationError -> {
         if (isOkOrCreated(builder)) {
            ProtoSchema protoSchema = new ProtoSchema();
            protoSchema.name = schemaName;
            if (validationError != null) {
               protoSchema.error = createErrorContent(schemaName, (String) validationError);
            }
            addEntityAsJson(protoSchema, builder, invocationHelper);
         }
         return builder.build();
      });
   }

   private boolean isOkOrCreated(NettyRestResponse.Builder builder) {
      return builder.getHttpStatus() == HttpResponseStatus.CREATED || builder.getHttpStatus() == HttpResponseStatus.OK;
   }

   private CompletionStage<RestResponse> getSchema(RestRequest request) {
      String schemaName = checkMandatorySchemaName(request);

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager()
            .getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);

      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      return restCacheManager.getPrivilegedInternalEntry(cache, schemaName, true).thenApply(entry -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         if(entry == null) {
            responseBuilder.status(HttpResponseStatus.NOT_FOUND.code());
         } else {
            responseBuilder.status(HttpResponseStatus.OK.code());
            responseBuilder.contentType(MediaType.TEXT_PLAIN);
            responseBuilder.entity(entry.getValue());
         }
         return responseBuilder.build();
      });
   }

   private CompletionStage<RestResponse> deleteSchema(RestRequest request) {
      String schemaName = checkMandatorySchemaName(request);

      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      AdvancedCache<Object, Object> protobufCache = restCacheManager.getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);

      return restCacheManager.getPrivilegedInternalEntry(protobufCache, schemaName, true).thenCompose(entry -> {
         NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
               responseBuilder.status(HttpResponseStatus.NO_CONTENT.code());
               return restCacheManager.remove(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, schemaName,
                     MediaType.MATCH_ALL, request)
                     .thenApply(v -> responseBuilder.build());
         }
         return CompletableFuture.completedFuture(responseBuilder.build());
      });
   }

   private ValidationError createErrorContent(String schemaName, String cause) {
      String message = "Schema " + schemaName + " has errors";
      ValidationError validationError = new ValidationError();
      validationError.message = message;
      validationError.cause = cause;
      return validationError;
   }

   private String checkMandatorySchemaName(RestRequest request) {
      String schemaName = request.variables().get("schemaName");
      if (schemaName == null)
         throw new NoKeyException("schemaName");
      return schemaName.endsWith(ProtobufMetadataManager.PROTO_KEY_SUFFIX) ? schemaName : schemaName + ProtobufMetadataManager.PROTO_KEY_SUFFIX;
   }

   static class ValidationError {
      public String message;
      public String cause;
   }
   static class ProtoSchema {
      public String name;
      public ValidationError error;
   }
}
