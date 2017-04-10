package org.infinispan.rest.server.operations.exceptions;

import org.infinispan.rest.server.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoDataFoundException extends RestResponseException {

   public NoDataFoundException() {
      super(HttpResponseStatus.BAD_REQUEST, "No data supplied");
   }

}
