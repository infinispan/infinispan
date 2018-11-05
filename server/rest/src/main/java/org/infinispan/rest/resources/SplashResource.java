package org.infinispan.rest.resources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.operations.exceptions.ServerInternalException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Handles the welcome page of the REST endpoint.
 *
 * @since 10.0
 */
public class SplashResource implements ResourceHandler {

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().method(Method.GET).path("/").handleWith(req -> this.serveStaticResource(req, "index.html"))
            .invocation().method(Method.GET).path("/banner.png").handleWith(req -> this.serveStaticResource(req, "banner.png"))
            .create();
   }

   private NettyRestResponse serveStaticResource(RestRequest request, String resource) throws ServerInternalException {
      NettyRestResponse.Builder responseBuilder = new NettyRestResponse.Builder();
      try {
         URL staticResource = SplashResource.class.getClassLoader().getResource(resource);
         return responseBuilder.entity(loadFile(staticResource))
               .contentType(getMediaType(resource))
               .status(HttpResponseStatus.OK)
               .build();
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
   }

   private MediaType getMediaType(String resource) {
      if (resource == null) return null;
      if (resource.endsWith(".html") || resource.endsWith(".htm")) return MediaType.TEXT_HTML;
      if (resource.endsWith(".png")) return MediaType.IMAGE_PNG;
      return null;
   }

   private byte[] loadFile(URL url) throws IOException {
      try (ByteArrayOutputStream buffer = new ByteArrayOutputStream(); InputStream is = url.openStream()) {
         int nRead;
         byte[] data = new byte[1024];
         while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
         }
         buffer.flush();
         return buffer.toByteArray();
      }
   }

}
