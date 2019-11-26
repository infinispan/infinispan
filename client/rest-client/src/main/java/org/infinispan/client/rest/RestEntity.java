package org.infinispan.client.rest;

import java.io.File;

import org.infinispan.client.rest.impl.okhttp.ByteArrayRestEntityOkHttp;
import org.infinispan.client.rest.impl.okhttp.FileRestEntityOkHttp;
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
}
