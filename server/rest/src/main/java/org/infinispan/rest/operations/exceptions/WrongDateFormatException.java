package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class WrongDateFormatException extends RestResponseException {

   public WrongDateFormatException(String description) {
      super(HttpResponseStatus.BAD_REQUEST, description);
   }

}
