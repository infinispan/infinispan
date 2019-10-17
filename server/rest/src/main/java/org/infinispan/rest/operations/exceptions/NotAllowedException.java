package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.0
 */
public class NotAllowedException extends RestResponseException {

   public NotAllowedException() {
      super(HttpResponseStatus.METHOD_NOT_ALLOWED, "Not allowed");
   }

}
