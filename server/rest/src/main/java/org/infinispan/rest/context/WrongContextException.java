package org.infinispan.rest.context;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exception indicating wrong context.
 */
public class WrongContextException extends RestResponseException {

   public WrongContextException() {
      super(HttpResponseStatus.NOT_FOUND, "Wrong context");
   }
}
