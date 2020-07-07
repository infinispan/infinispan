package org.infinispan.rest;

import org.infinispan.commons.util.Util;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An exception representing non-critical HTTP processing errors which will be translated
 * into {@link org.infinispan.rest.framework.RestResponse} and sent back to the client.
 *
 * <p>
 *    {@link RestRequestHandler} and {@link RestRequestHandler} are responsible for catching subtypes of this
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
    * Creates a new {@link RestResponseException}.
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

   /**
    * Creates a new {@link RestResponseException} whose status is 500.
    *
    * @param t Throwable instance.
    */
   public RestResponseException(Throwable t) {
      this(HttpResponseStatus.INTERNAL_SERVER_ERROR, Util.getRootCause(t));
   }

   /**
    * Creates a new {@link RestResponseException}.
    *
    * @param status Status code returned to the client.
    * @param t Throwable instance.
    */
   public RestResponseException(HttpResponseStatus status, Throwable t) {
      this(status, t.getMessage(), t);
   }

   public HttpResponseStatus getStatus() {
      return status;
   }

   public String getText() {
      return text;
   }
}
