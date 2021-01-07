package org.infinispan.client.rest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.infinispan.client.rest.impl.okhttp.ByteArrayRestEntityOkHttp;
import org.infinispan.client.rest.impl.okhttp.FileRestEntityOkHttp;
import org.infinispan.client.rest.impl.okhttp.InputStreamEntityOkHttp;
import org.infinispan.client.rest.impl.okhttp.StringRestEntityOkHttp;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestEntity {
   String getBody();

   MediaType contentType();

   static RestEntity create(MediaType contentType, String body) {
      return new StringRestEntityOkHttp(contentType, body);
   }

   static RestEntity create(MediaType contentType, byte[] body) {
      return new ByteArrayRestEntityOkHttp(contentType, body);
   }

   static RestEntity create(MediaType contentType, File file) {
      return new FileRestEntityOkHttp(contentType, file);
   }

   static RestEntity create(MediaType contentType, InputStream inputStream) {
      return new InputStreamEntityOkHttp(contentType, inputStream);
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
