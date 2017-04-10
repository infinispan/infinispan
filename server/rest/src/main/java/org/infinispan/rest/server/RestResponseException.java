package org.infinispan.rest.server;

import io.netty.handler.codec.http.HttpResponseStatus;

public class RestResponseException extends Exception {

   private final HttpResponseStatus status;
   private final String text;

   public RestResponseException(HttpResponseStatus status, String text) {
      this.status = status;
      this.text = text;
   }

   public RestResponseException(HttpResponseStatus status, String text, Throwable t) {
      super(t);
      this.status = status;
      this.text = text;
   }

   public InfinispanResponse toResponse() {
      return InfinispanResponse.asError(status, text);
   }

}
