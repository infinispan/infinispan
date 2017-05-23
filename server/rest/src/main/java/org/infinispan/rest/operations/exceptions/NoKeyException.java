package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoKeyException extends RestResponseException {

   public NoKeyException() {
      super(HttpResponseStatus.BAD_REQUEST, "No key specified");
   }

}
