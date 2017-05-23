package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoDataFoundException extends RestResponseException {

   public NoDataFoundException() {
      super(HttpResponseStatus.BAD_REQUEST, "No data supplied");
   }

}
