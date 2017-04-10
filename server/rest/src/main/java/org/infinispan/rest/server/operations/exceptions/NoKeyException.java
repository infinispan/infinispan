package org.infinispan.rest.server.operations.exceptions;

import org.infinispan.rest.server.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoKeyException extends RestResponseException {

   public NoKeyException() {
      super(HttpResponseStatus.BAD_REQUEST, "No key specified");
   }

}
