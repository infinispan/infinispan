package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;
import static org.infinispan.rest.resources.ResourceUtil.response;
import static org.infinispan.rest.resources.ResourceUtil.responseFuture;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.rest.NettyRestRequest;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.logging.Log;
import org.infinispan.server.core.BackupManager;
import org.infinispan.server.core.backup.BackupManagerResources;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.DiskAttribute;
import io.netty.handler.codec.http.multipart.DiskFileUpload;
import io.netty.handler.codec.http.multipart.HttpPostMultipartRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpPostRequestDecoder;

/**
 * A helper class for common functionality related to the {@link BackupManager}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
class BackupManagerResource {

   private static final Log LOG = LogFactory.getLog(BackupManagerResource.class, Log.class);

   private static final String DIR_KEY = "directory";
   private static final String LOCATION_KEY = "location";
   private static final String RESOURCES_KEY = "resources";

   static CompletionStage<RestResponse> handleBackupRequest(RestRequest request, BackupManager backupManager,
                                                            TriConsumer<String, Path, Json> creationConsumer) {
      String name = request.variables().get("backupName");
      Method method = request.method();
      switch (method) {
         case DELETE:
            return handleDeleteBackup(name, backupManager);
         case GET:
         case HEAD:
            return handleGetBackup(name, backupManager, method);
         case POST:
            return handleCreateBackup(name, request, backupManager, creationConsumer);
         default:
            return responseFuture(BAD_REQUEST, "Unsupported request method " + method);
      }
   }

   private static CompletionStage<RestResponse> handleCreateBackup(String name, RestRequest request, BackupManager backupManager,
                                                                   TriConsumer<String, Path, Json> creationConsumer) {
      BackupManager.Status existingStatus = backupManager.getBackupStatus(name);
      if (existingStatus != BackupManager.Status.NOT_FOUND)
         return responseFuture(CONFLICT);

      Json json = Json.read(request.contents().asString());
      Json dirJson = json.at(DIR_KEY);
      Path workingDIr = dirJson == null ? null : Paths.get(dirJson.asString());
      if (workingDIr != null && !Files.isDirectory(workingDIr))
         return responseFuture(BAD_REQUEST, String.format("'%s' must be a directory", DIR_KEY));

      Json requestsJson = json.at(RESOURCES_KEY);
      creationConsumer.accept(name, workingDIr, requestsJson);
      return responseFuture(ACCEPTED);
   }

   private static CompletionStage<RestResponse> handleDeleteBackup(String name, BackupManager backupManager) {
      return backupManager.removeBackup(name).handle((s, t) -> {
         if (t != null)
            return response(INTERNAL_SERVER_ERROR, t.getMessage());

         switch (s) {
            case NOT_FOUND:
               return response(NOT_FOUND);
            case IN_PROGRESS:
               return response(ACCEPTED);
            case COMPLETE:
               return response(NO_CONTENT);
            default:
               return response(INTERNAL_SERVER_ERROR);
         }
      });
   }

   private static CompletionStage<RestResponse> handleGetBackup(String name, BackupManager backupManager, Method method) {
      BackupManager.Status status = backupManager.getBackupStatus(name);
      switch (status) {
         case FAILED:
            return responseFuture(INTERNAL_SERVER_ERROR);
         case NOT_FOUND:
            return responseFuture(NOT_FOUND);
         case IN_PROGRESS:
            return responseFuture(ACCEPTED);
         default:
            File zip = backupManager.getBackupLocation(name).toFile();
            NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
            responseBuilder
                  .contentType(MediaType.APPLICATION_ZIP)
                  .header("Content-Disposition", String.format("attachment; filename=%s", zip.getName()))
                  .contentLength(zip.length());

            if (method == Method.GET)
               responseBuilder.entity(zip);
            return CompletableFuture.completedFuture(responseBuilder.build());
      }
   }

   static CompletionStage<RestResponse> handleRestoreRequest(RestRequest request, BiFunction<Path, Json, CompletionStage<Void>> function) {
      Path path;
      Json resourcesJson = Json.object();
      MediaType contentType = request.contentType();
      boolean uploadedBackup = contentType.match(MediaType.MULTIPART_FORM_DATA);
      try {
         if (uploadedBackup) {
            FullHttpRequest nettyRequest = ((NettyRestRequest) request).getFullHttpRequest();
            DefaultHttpDataFactory factory = new DefaultHttpDataFactory(true);
            InterfaceHttpPostRequestDecoder decoder = new HttpPostMultipartRequestDecoder(factory, nettyRequest);
            DiskFileUpload backup = (DiskFileUpload) decoder.getBodyHttpData("backup");
            path = backup.getFile().toPath();

            DiskAttribute resources = (DiskAttribute) decoder.getBodyHttpData("resources");
            if (resources != null)
               resourcesJson = Json.read(resources.getString());
         } else if (contentType.match(MediaType.APPLICATION_JSON)){
            // Attempt to parse body as json
            Json json = Json.read(request.contents().asString());
            Json resources = json.at(RESOURCES_KEY);
            if (resources != null)
               resourcesJson = resources;

            Json backupPath = json.at(LOCATION_KEY);
            if (backupPath == null)
               return responseFuture(BAD_REQUEST, "Required json attribute 'backup-location' not found");

            path = Paths.get(backupPath.asString());
         } else {
            return responseFuture(UNSUPPORTED_MEDIA_TYPE);
         }

         return function.apply(path, resourcesJson).handle((Void, t) -> {
                  if (uploadedBackup) {
                     try {
                        Files.delete(path);
                     } catch (IOException e) {
                        LOG.warnf(e, "Unable to delete uploaded backup file '%s'", path);
                     }
                  }
                  return t != null ?
                        response(INTERNAL_SERVER_ERROR, t.getMessage()) :
                        response(NO_CONTENT);
               }
         );
      } catch (IOException e) {
         LOG.error(e);
         return responseFuture(INTERNAL_SERVER_ERROR, e.getMessage());
      }
   }

   static BackupManager.Resources getResources(Json json) {
      BackupManagerResources.Builder builder = new BackupManagerResources.Builder();
      if (json == null || json.isNull())
         return builder.includeAll().build();

      Map<String, Object> jsonMap = json.asMap();
      if (jsonMap.isEmpty())
         return builder.includeAll().build();

      for (Map.Entry<String, Object> e : jsonMap.entrySet()) {
         @SuppressWarnings("unchecked")
         List<String> resources = (List<String>) e.getValue();
         BackupManager.Resources.Type type = BackupManager.Resources.Type.fromString(e.getKey());
         if (resources.size() == 1 && resources.get(0).equals("*")) {
            builder.includeAll(type);
         } else {
            builder.addResources(type, resources);
         }
      }
      return builder.build();
   }
}
