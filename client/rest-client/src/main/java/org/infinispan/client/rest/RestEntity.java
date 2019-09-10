package org.infinispan.client.rest;

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
}
