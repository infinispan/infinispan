package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.util.common.function.TriFunction;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.InvocationHelper;
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

   static CompletionStage<RestResponse> handleBackupRequest(InvocationHelper invocationHelper, RestRequest request, BackupManager backupManager,
                                                            TriConsumer<String, Path, Json> creationConsumer) {
      String name = request.variables().get("backupName");
      Method method = request.method();
      switch (method) {
         case DELETE:
            return handleDeleteBackup(invocationHelper, name, request, backupManager);
         case GET:
         case HEAD:
            return handleGetBackup(invocationHelper, name, request, backupManager, method);
         case POST:
            return handleCreateBackup(invocationHelper, name, request, backupManager, creationConsumer);
         default:
            throw Log.REST.wrongMethod(method.toString());
      }
   }

   private static CompletionStage<RestResponse> handleCreateBackup(InvocationHelper invocationHelper, String name, RestRequest request, BackupManager backupManager,
                                                                   TriConsumer<String, Path, Json> creationConsumer) {
      BackupManager.Status existingStatus = backupManager.getBackupStatus(name);
      if (existingStatus != BackupManager.Status.NOT_FOUND)
         return invocationHelper.newResponse(request, CONFLICT).toFuture();

      String body = request.contents().asString();
      Json json = !body.isEmpty() ? Json.read(body) : Json.object();
      Json dirJson = json.at(DIR_KEY);
      Path workingDir = dirJson == null ? null : Paths.get(dirJson.asString());
      if (workingDir != null && !Files.isDirectory(workingDir)) {
         throw Log.REST.notADirectory(dirJson.asString());
      }

      Json requestsJson = json.at(RESOURCES_KEY);
      creationConsumer.accept(name, workingDir, requestsJson);
      return invocationHelper.newResponse(request, ACCEPTED).toFuture();
   }

   private static CompletionStage<RestResponse> handleDeleteBackup(InvocationHelper invocationHelper, String name, RestRequest request, BackupManager backupManager) {
      return backupManager.removeBackup(name).handle((BackupManager.Status s, Throwable t) -> handleDelete(invocationHelper, request, s, t));
   }

   private static CompletionStage<RestResponse> handleGetBackup(InvocationHelper invocationHelper, String name, RestRequest request, BackupManager backupManager, Method method) {
      BackupManager.Status status = backupManager.getBackupStatus(name);
      switch (status) {
         case FAILED:
            throw Log.REST.backupFailed();
         case NOT_FOUND:
            return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
         case IN_PROGRESS:
            return invocationHelper.newResponse(request, ACCEPTED).toFuture();
         default:
            File zip = backupManager.getBackupLocation(name).toFile();
            NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request)
                  .contentType(MediaType.APPLICATION_ZIP)
                  .header("Content-Disposition", String.format("attachment; filename=%s", zip.getName()))
                  .contentLength(zip.length());

            if (method == Method.GET)
               responseBuilder.entity(zip);
            return CompletableFuture.completedFuture(responseBuilder.build());
      }
   }

   static CompletionStage<RestResponse> handleRestoreRequest(InvocationHelper invocationHelper, RestRequest request, BackupManager backupManager,
                                                             TriFunction<String, Path, Json, CompletionStage<Void>> function) {
      String name = request.variables().get("restoreName");
      Method method = request.method();
      switch (method) {
         case DELETE:
            return handleDeleteRestore(invocationHelper, name, request, backupManager);
         case HEAD:
            return handleRestoreStatus(invocationHelper, name, request, backupManager);
         case POST:
            return handleRestore(invocationHelper, name, request, backupManager, function);
         default:
            throw Log.REST.wrongMethod(method.toString());
      }
   }

   static CompletionStage<RestResponse> handleDeleteRestore(InvocationHelper invocationHelper, String name, RestRequest request, BackupManager backupManager) {
      return backupManager.removeRestore(name).handle((BackupManager.Status s, Throwable t) -> handleDelete(invocationHelper, request, s, t));
   }

   static CompletionStage<RestResponse> handleRestoreStatus(InvocationHelper invocationHelper, String name, RestRequest request, BackupManager backupManager) {
      BackupManager.Status status = backupManager.getRestoreStatus(name);
      switch (status) {
         case NOT_FOUND:
            return invocationHelper.newResponse(request, NOT_FOUND).toFuture();
         case IN_PROGRESS:
            return invocationHelper.newResponse(request, ACCEPTED).toFuture();
         case COMPLETE:
            return invocationHelper.newResponse(request, CREATED).toFuture();
         default:
            throw Log.REST.restoreFailed();
      }
   }

   static CompletionStage<RestResponse> handleRestore(InvocationHelper invocationHelper, String name, RestRequest request, BackupManager backupManager,
                                                      TriFunction<String, Path, Json, CompletionStage<Void>> function) {
      BackupManager.Status existingStatus = backupManager.getRestoreStatus(name);
      if (existingStatus != BackupManager.Status.NOT_FOUND)
         return invocationHelper.newResponse(request, CONFLICT).toFuture();

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
         } else if (contentType.match(MediaType.APPLICATION_JSON)) {
            // Attempt to parse body as json
            Json json = Json.read(request.contents().asString());
            Json resources = json.at(RESOURCES_KEY);
            if (resources != null)
               resourcesJson = resources;

            Json backupPath = json.at(LOCATION_KEY);
            if (backupPath == null) {
               throw Log.REST.missingArgument("backup-location");
            }

            path = Paths.get(backupPath.asString());
         } else {
            return invocationHelper.newResponse(request, UNSUPPORTED_MEDIA_TYPE).toFuture();
         }

         function.apply(name, path, resourcesJson).whenComplete((Void, t) -> {
                  if (t != null) {
                     LOG.error(t);
                  }
                  if (uploadedBackup) {
                     try {
                        Files.delete(path);
                     } catch (IOException e) {
                        LOG.warnf(e, "Unable to delete uploaded backup file '%s'", path);
                     }
                  }
               }
         );
         return invocationHelper.newResponse(request, ACCEPTED).toFuture();
      } catch (IOException e) {
         throw Util.unchecked(e);
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

   private static RestResponse handleDelete(InvocationHelper invocationHelper, RestRequest request, BackupManager.Status s, Throwable t) {
      if (t != null) {
         throw Util.unchecked(t);
      }
      switch (s) {
         case NOT_FOUND:
            return invocationHelper.newResponse(request, NOT_FOUND);
         case IN_PROGRESS:
            return invocationHelper.newResponse(request, ACCEPTED);
         case COMPLETE:
            return invocationHelper.newResponse(request, NO_CONTENT);
         default:
            throw Log.REST.backupDeleteFailed();
      }
   }
}
