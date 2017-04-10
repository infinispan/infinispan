package org.infinispan.rest.server.operations.exceptions;

import org.infinispan.rest.server.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoMediaTypeException extends RestResponseException {

   public NoMediaTypeException() {
      super(HttpResponseStatus.NOT_ACCEPTABLE, "No media type specified");
   }
}
