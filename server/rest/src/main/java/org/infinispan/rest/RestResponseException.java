package org.infinispan.rest;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception representing non-critical HTTP processing errors which will be translated
 * into {@link org.infinispan.rest.framework.RestResponse} and sent back to the client.
 *
 * <p>
 *    {@link Http20RequestHandler} and {@link Http11RequestHandler} are responsible for catching subtypes of this
 *    exception and translate them into proper Netty responses.
 * </p>
 */
public class RestResponseException extends RuntimeException {

   private final HttpResponseStatus status;
   private final String text;

   /**
    * Creates new {@link RestResponseException}.
    *
    * @param status Status code returned to the client.
    * @param text Text returned to the client.
    */
   public RestResponseException(HttpResponseStatus status, String text) {
      this.status = status;
      this.text = text;
   }

   /**
    * Creates new {@link RestResponseException}.
    *
    * @param status Status code returned to the client.
    * @param text Text returned to the client.
    * @param t Throwable instance.
    */
   public RestResponseException(HttpResponseStatus status, String text, Throwable t) {
      super(t);
      this.status = status;
      this.text = text;
   }

   public HttpResponseStatus getStatus() {
      return status;
   }

   public String getText() {
      return text;
   }
}
