package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ServerInternalException extends RestResponseException {

   public ServerInternalException(Exception e) {
      super(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal server error occurred", e);
   }
}
