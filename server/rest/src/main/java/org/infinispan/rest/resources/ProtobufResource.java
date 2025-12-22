package org.infinispan.rest.resources;

import static org.infinispan.rest.framework.Method.DELETE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.framework.Method.PUT;
import static org.infinispan.rest.resources.ResourceUtil.addEntityAsJson;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponse;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;
import static org.infinispan.rest.resources.ResourceUtil.isPretty;

import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.client.hotrod.RemoteSchemasAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.core.query.impl.ProtobufMetadataManagerImpl;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestRequestHandler;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.operations.exceptions.NoDataFoundException;
import org.infinispan.rest.operations.exceptions.NoKeyException;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.telemetry.InfinispanTelemetry;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Protobuf schema manipulation Resource
 *
 * @author Katia Aresti
 * @since 11
 */
public class ProtobufResource extends BaseCacheResource implements ResourceHandler {

   public ProtobufResource(InvocationHelper invocationHelper, InfinispanTelemetry telemetryService) {
      super(invocationHelper, telemetryService);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("protobuf", "REST resource to manage Protobuf schemas.")
            // Key related operations
            .invocation().methods(GET).path("/v2/schemas").handleWith(this::getSchemasNames)
            .invocation().methods(GET).path("/v2/schemas").withAction("types").handleWith(this::getTypes)
            .invocation().methods(POST).path("/v2/schemas/{schemaName}")
               .permission(AuthorizationPermission.CREATE).auditContext(AuditContext.SERVER).name("SCHEMA CREATE")
               .handleWith(r -> createOrReplace(r, true))
            .invocation().methods(PUT).path("/v2/schemas/{schemaName}")
               .permission(AuthorizationPermission.CREATE).auditContext(AuditContext.SERVER).name("SCHEMA CREATE")
               .handleWith(r -> createOrReplace(r, false))
            .invocation().methods(GET).path("/v2/schemas/{schemaName}").handleWith(this::getSchema)
            .invocation().method(DELETE).path("/v2/schemas/{schemaName}")
               .permission(AuthorizationPermission.CREATE).auditContext(AuditContext.SERVER).name("SCHEMA DELETE")
               .handleWith(this::deleteSchema)
            .create();
   }

   private CompletionStage<RestResponse> getSchemasNames(RestRequest request) {
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager()
            .getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME, request);
      boolean pretty = isPretty(request);

      return CompletableFuture.supplyAsync(() ->
                  Flowable.fromIterable(cache.keySet())
                        .filter(key -> !((String) key).endsWith(RemoteSchemasAdmin.SchemaErrors.ERRORS_KEY_SUFFIX))
                        .map(key -> {
                           String error = (String) cache
                                 .get(key + RemoteSchemasAdmin.SchemaErrors.ERRORS_KEY_SUFFIX);
                           ProtoSchema protoSchema = new ProtoSchema();
                           protoSchema.name = (String) key;
                           if (error != null) {
                              protoSchema.error = createErrorContent(protoSchema.name, error);
                           }
                           return protoSchema;
                        })
                        .sorted(Comparator.comparing(s -> s.name))
                        .collect(Collectors.toList())
                        .map(protoSchemas -> asJsonResponse(invocationHelper.newResponse(request), Json.make(protoSchemas), pretty))
                        .toCompletionStage()
            , invocationHelper.getExecutor())
            .thenCompose(Function.identity());
   }

   private CompletionStage<RestResponse> createOrReplace(RestRequest request, boolean create) {

      String schemaName = checkMandatorySchemaName(request);

      ContentSource contents = request.contents();
      if (contents == null || contents.size() == 0) throw new NoDataFoundException("Schema data not sent in the request");

      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager()
            .getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME, request);

      NettyRestResponse.Builder builder = invocationHelper.newResponse(request);

      CompletableFuture<Object> putSchema;
      if (create) {
         putSchema = cache.putIfAbsentAsync(schemaName, contents.asString()).whenComplete((result, ex) -> {
            if (ex != null) {
               builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR).entity(RestRequestHandler.filterCause(ex));
            } else if (result == null) {
               builder.status(HttpResponseStatus.CREATED);
            } else {
               builder.status(HttpResponseStatus.CONFLICT);
            }
         });
      } else {
         putSchema = cache.putAsync(schemaName, contents.asString())
                 .whenComplete((result, ex) -> {
                    if (ex == null) {
                       builder.status(HttpResponseStatus.OK);
                    } else {
                       builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                               .entity(RestRequestHandler.filterCause(ex));
                    }
                 });
      }

      return putSchema.thenCompose(r -> {
         if (isOkOrCreated(builder)) {
            return cache
                    .getAsync(schemaName + ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX)
                    .thenApply(err -> err)
                    .exceptionally( ex -> {
                       builder.status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                               .entity(RestRequestHandler.filterCause(ex));
                       return CompletableFutures.completedNull();
                    });
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
            addEntityAsJson(protoSchema, builder);
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
            .getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME, request);

      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      return restCacheManager.getPrivilegedInternalEntry(cache, schemaName, true).thenApply(entry -> {
         NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
         if (entry == null) {
            responseBuilder.status(HttpResponseStatus.NOT_FOUND);
         } else {
            responseBuilder.status(HttpResponseStatus.OK);
            responseBuilder.contentType(MediaType.TEXT_PLAIN);
            responseBuilder.entity(entry.getValue());
         }
         return responseBuilder.build();
      });
   }

   private CompletionStage<RestResponse> getTypes(RestRequest request) {
      ProtobufMetadataManagerImpl protobufMetadataManager = (ProtobufMetadataManagerImpl) invocationHelper.protobufMetadataManager();
      Set<String> knownTypes = protobufMetadataManager.getKnownTypes();
      Json protobufTypes = Json.array();
      for (String type: knownTypes) {
         protobufTypes.add(type);
      }
      return asJsonResponseFuture(invocationHelper.newResponse(request), protobufTypes, isPretty(request));
   }

   private CompletionStage<RestResponse> deleteSchema(RestRequest request) {
      String schemaName = checkMandatorySchemaName(request);

      RestCacheManager<Object> restCacheManager = invocationHelper.getRestCacheManager();
      AdvancedCache<Object, Object> protobufCache = restCacheManager.getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME, request);

      return restCacheManager.getPrivilegedInternalEntry(protobufCache, schemaName, true).thenCompose(entry -> {
         NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
         responseBuilder.status(HttpResponseStatus.NOT_FOUND);

         if (entry instanceof InternalCacheEntry) {
            responseBuilder.status(HttpResponseStatus.NO_CONTENT);
            return restCacheManager.remove(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME, schemaName,
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

   static class ValidationError implements JsonSerialization {
      public String message;
      public String cause;

      @Override
      public Json toJson() {
         return Json.object().set("message", message).set("cause", cause);
      }
   }

   static class ProtoSchema implements JsonSerialization {
      public String name;
      public ValidationError error;

      @Override
      public Json toJson() {
         return Json.object("name", name).set("error", error == null ? null : error.toJson());
      }
   }
}
