package org.infinispan.client.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.infinispan.client.rest.impl.jdk.form.MultiPartRestEntityJDK;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestEntity {
   RestEntity EMPTY = new RestEntity() {
      @Override
      public HttpRequest.BodyPublisher bodyPublisher() {
         return HttpRequest.BodyPublishers.noBody();
      }

      @Override
      public MediaType contentType() {
         return null;
      }
   };

   HttpRequest.BodyPublisher bodyPublisher();

   MediaType contentType();

   static RestEntity empty() {
      return EMPTY;
   }

   static RestEntity create(MediaType contentType, String body) {
      return new RestEntity() {
         @Override
         public HttpRequest.BodyPublisher bodyPublisher() {
            return HttpRequest.BodyPublishers.ofString(body);
         }

         @Override
         public MediaType contentType() {
            return contentType;
         }
      };
   }

   static RestEntity create(MediaType contentType, byte[] body) {
      return new RestEntity() {
         @Override
         public HttpRequest.BodyPublisher bodyPublisher() {
            return HttpRequest.BodyPublishers.ofByteArray(body);
         }

         @Override
         public MediaType contentType() {
            return contentType;
         }
      };
   }

   static RestEntity create(MediaType contentType, File file) {
      return new RestEntity() {
         @Override
         public HttpRequest.BodyPublisher bodyPublisher() {
            try {
               return HttpRequest.BodyPublishers.ofFile(file.toPath());
            } catch (FileNotFoundException e) {
               throw new RuntimeException(e);
            }
         }

         @Override
         public MediaType contentType() {
            return contentType;
         }
      };
   }

   static RestEntity create(MediaType contentType, InputStream inputStream) {
      return new RestEntity() {
         @Override
         public HttpRequest.BodyPublisher bodyPublisher() {
            return HttpRequest.BodyPublishers.ofInputStream(() -> inputStream);
         }

         @Override
         public MediaType contentType() {
            return contentType;
         }
      };
   }

   static RestEntity form(Map<String, List<String>> formData) {
      StringBuilder fb = new StringBuilder();
      for (Map.Entry<String, List<String>> singleEntry : formData.entrySet()) {
         for (String v : singleEntry.getValue()) {
            if (fb.length() > 0) {
               fb.append("&");
            }
            fb.append(URLEncoder.encode(singleEntry.getKey(), StandardCharsets.UTF_8));
            fb.append("=");
            fb.append(URLEncoder.encode(v, StandardCharsets.UTF_8));
         }
      }
      return create(MediaType.APPLICATION_WWW_FORM_URLENCODED, fb.toString());
   }

   static MultiPartRestEntity multiPart() {
      return new MultiPartRestEntityJDK();
   }

   static RestEntity create(File file) {
      if (file.getName().endsWith(".yaml") || file.getName().endsWith(".yml")) {
         // We can only "guess" YAML by its extension
         return RestEntity.create(MediaType.APPLICATION_YAML, file);
      }
      try (InputStream is = new FileInputStream(file)) {
         int b;
         while ((b = is.read()) > -1) {
            if (b == '{') {
               return RestEntity.create(MediaType.APPLICATION_JSON, file);
            } else if (b == '<') {
               return RestEntity.create(MediaType.APPLICATION_XML, file);
            }
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      return RestEntity.create(MediaType.APPLICATION_OCTET_STREAM, file);
   }
}
