package org.infinispan.rest.operations;

import static org.infinispan.commons.dataconversion.MediaType.IMAGE_PNG;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_HTML;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;

import org.infinispan.rest.InfinispanCacheResponse;
import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;
import org.infinispan.rest.operations.exceptions.ServerInternalException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Class responsible for serving static content
 */
public class StaticContent {

   private static final URL HTML_FILE = StaticContent.class.getClassLoader().getResource("index.html");
   private static final URL BANNER_FILE = StaticContent.class.getClassLoader().getResource("banner.png");

   public InfinispanResponse serveHtmlFile(InfinispanRequest request) throws ServerInternalException {
      InfinispanResponse response = InfinispanCacheResponse.inReplyTo(request);
      try {
         response.contentAsBytes(loadFile(HTML_FILE));
         response.contentType(TEXT_HTML.toString());
         response.status(HttpResponseStatus.OK);
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
      return response;
   }

   public InfinispanResponse serveBannerFile(InfinispanRequest request) throws ServerInternalException {
      InfinispanResponse response = InfinispanCacheResponse.inReplyTo(request);
      try {
         response.contentAsBytes(loadFile(BANNER_FILE));
         response.contentType(IMAGE_PNG.toString());
         response.status(HttpResponseStatus.OK);
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
      return response;
   }

   private byte[] loadFile(URL url) throws IOException, URISyntaxException {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      try (InputStream is = url.openStream()) {
         int nRead;
         byte[] data = new byte[1024];
         while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
         }
         buffer.flush();
         return buffer.toByteArray();
      } finally {
         buffer.close();
      }
   }

}
