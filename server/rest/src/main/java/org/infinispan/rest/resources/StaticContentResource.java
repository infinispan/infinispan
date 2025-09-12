package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.MOVED_PERMANENTLY;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OCTET_STREAM_TYPE;
import static org.infinispan.rest.framework.Method.GET;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaTypeResolver;
import org.infinispan.rest.DateUtils;
import org.infinispan.rest.InvocationHelper;
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
   public static final String DEFAULT_RESOURCE = "index.html";
   public static final String CONFIG_RESOURCE = "config.js";
   private final String noFileUri;
   private final ResourceResolver customResourceResolver;
   private final String rootUri;
   private final InvocationHelper invocationHelper;

   public interface ResourceResolver {
      /**
       * Resolves a path to a resource name
       *
       * @param path The requested URL path
       * @param resource the {@link StaticContentResource} instance
       * @return The resource name
       */
      String rewrite(String path, StaticContentResource resource);
   }

   /**
    * @param dir The path to serve files from
    * @param urlPath The url path to serve the file.
    * @param resourceResolver a {@link ResourceResolver} to resolve requests into resources
    */
   public StaticContentResource(InvocationHelper invocationHelper, Path dir, String urlPath, ResourceResolver resourceResolver) {
      this.invocationHelper = invocationHelper;
      this.dir = dir.toAbsolutePath();
      this.urlPath = urlPath;
      this.noFileUri = "/" + urlPath;
      this.customResourceResolver = resourceResolver;
      this.rootUri = noFileUri + "/";
   }

   public StaticContentResource(InvocationHelper invocationHelper, Path dir, String urlPath) {
      this(invocationHelper, dir, urlPath, null);
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("static", "REST endpoint for static resources.")
            .invocation().methods(GET).anonymous(true).path(urlPath + "/").path(urlPath + "/*").handleWith(this::serveFile)
            .create();
   }

   private File resolve(String resource) {
      Path resolved = dir.resolve(resource);
      try {
         if (!resolved.toFile().getCanonicalPath().startsWith(dir.toFile().getCanonicalPath())) {
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
      NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(restRequest);
      String uri = restRequest.uri();

      if (uri.equals(noFileUri)) {
         return completedFuture(responseBuilder.location(rootUri).status(MOVED_PERMANENTLY).build());
      }

      String resource = uri.substring(uri.indexOf(urlPath) + urlPath.length() + 1);

      if (customResourceResolver != null) {
         resource = customResourceResolver.rewrite(resource, this);
      } else {
         if (uri.equals(rootUri)) resource = DEFAULT_RESOURCE;
      }

      File file = resolve(resource);
      if (file == null) {
         return completedFuture(responseBuilder.status(HttpResponseStatus.NOT_FOUND).build());
      }

      String ifModifiedSince = restRequest.getIfModifiedSinceHeader();
      if (ifModifiedSince != null && !ifModifiedSince.isEmpty()) {
         boolean isNotModified = DateUtils.isNotModifiedSince(ifModifiedSince, file.lastModified());
         if (isNotModified) {
            responseBuilder.status(NOT_MODIFIED);
            return completedFuture(responseBuilder.build());
         }
      }

      responseBuilder.lastModified(file.lastModified());
      responseBuilder.header("Cache-control", "private, max-age=" + CACHE_MAX_AGE_SECONDS);

      Date now = new Date();
      responseBuilder.addProcessedDate(now);
      responseBuilder.header("Expires", DateUtils.toRFC1123(now.getTime() + TimeUnit.SECONDS.toMillis(CACHE_MAX_AGE_SECONDS)));

      String resolved = MediaTypeResolver.getMediaType(file.getName());
      String mediaType = resolved != null ? resolved : APPLICATION_OCTET_STREAM_TYPE;

       if (resource.equals(DEFAULT_RESOURCE)) {
           return loadIndexFileAndReplaceValue("/console/",
                   "{{INFINISPAN_BASE_PATH}}",
                   mediaType,
                   restRequest, file, responseBuilder);
       }

       if (resource.equals(CONFIG_RESOURCE)) {
           String restContextPath = "/" + invocationHelper.getConfiguration().contextPath();
           return loadIndexFileAndReplaceValue(restContextPath,
                   "{{INFINISPAN_REST_CONTEXT_PATH}}",
                   mediaType,
                   restRequest, file, responseBuilder);
       }

       responseBuilder
            .contentType(mediaType)
            .entity(file);
      return completedFuture(responseBuilder.build());
   }

    private CompletableFuture<RestResponse> loadIndexFileAndReplaceValue(String defaultBasePath,
                                                                         String remplacementName,
                                                                         String mediaType,
                                                                         RestRequest restRequest,
                                                                         File file,
                                                                         NettyRestResponse.Builder responseBuilder) {
        try {
            String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            String xFowardedPrefix = restRequest.header("X-Forwarded-Prefix");
            String currentBasePath = defaultBasePath;
            if (xFowardedPrefix != null && !xFowardedPrefix.isEmpty()) {
                currentBasePath = xFowardedPrefix + currentBasePath;
            }
            String processedContent = content
                    .replace(remplacementName, currentBasePath);
            responseBuilder
                    .contentType(mediaType)
                    .entity(processedContent);
            return completedFuture(responseBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
