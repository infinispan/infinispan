package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 10.0
 */
public class InvalidFlagException extends RestResponseException {

   public InvalidFlagException(Throwable error) {
      super(HttpResponseStatus.BAD_REQUEST, error);
   }

}
