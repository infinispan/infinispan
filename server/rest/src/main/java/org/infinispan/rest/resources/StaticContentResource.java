package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.rest.framework.Method.GET;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaTypeResolver;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * REST resource to serve static content.
 *
 * @since 10.0
 */
public class StaticContentResource implements ResourceHandler {

   private final Path dir;
   private final String urlPath;

   private static final int CACHE_MAX_AGE_SECONDS = 60 * 60 * 24 * 31;  // 1 Month
   private static final String DEFAULT_RESOURCE = "index.html";

   /**
    * @param dir The path to serve files from
    * @param urlPath The url path to serve the files
    */
   public StaticContentResource(Path dir, String urlPath) {
      this.dir = dir.toAbsolutePath();
      this.urlPath = urlPath;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET).path(urlPath + "/").path(urlPath + "/*").handleWith(this::serveFile)
            .create();
   }

   private File resolve(String resource) {
      Path resolved = dir.resolve(resource);
      try {
         if (!resolved.toFile().getCanonicalPath().startsWith(dir.toAbsolutePath().toString())) {
            return null;
         }
      } catch (IOException e) {
         return null;
      }
      File file = resolved.toFile();
      if (!file.isFile() || !file.exists()) {
         return null;
      }
      return file;
   }

   private CompletionStage<RestResponse> serveFile(RestRequest restRequest) {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();

      String uri = restRequest.uri();
      String resource = uri.equals("/" + urlPath) ? "" : uri.substring(uri.indexOf(urlPath) + urlPath.length() + 1);
      if (resource.isEmpty()) resource = DEFAULT_RESOURCE;

      File file = resolve(resource);
      if (file == null) {
         return CompletableFuture.completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND).build());
      }

      String ifModifiedSince = restRequest.getIfModifiedSinceHeader();
      if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
         boolean isNotModified = DateUtils.isNotModifiedSince(ifModifiedSince, file.lastModified());
         if (isNotModified) {
            responseBuilder.status(NOT_MODIFIED);
            return CompletableFuture.completedFuture(responseBuilder.build());
         }
      }

      responseBuilder.lastModified(file.lastModified());
      responseBuilder.header("Cache-control", "private, max-age=" + CACHE_MAX_AGE_SECONDS);
      responseBuilder.header("X-Frame-Options", "sameorigin");
      responseBuilder.header("X-XSS-Protection", "1; mode=block");
      responseBuilder.header("X-Content-Type-Options", "nosniff");

      Date now = new Date();
      responseBuilder.addProcessedDate(now);
      responseBuilder.header("Expires", DateUtils.toRFC1123(now.getTime() + TimeUnit.SECONDS.toMillis(CACHE_MAX_AGE_SECONDS)));

      String resolved = MediaTypeResolver.getMediaType(file.getName());
      String mediaType = resolved != null ? resolved : APPLICATION_OCTET_STREAM_TYPE;

      responseBuilder.contentLength(file.length())
            .contentType(mediaType)
            .entity(file);
      return CompletableFuture.completedFuture(responseBuilder.build());
   }
}
