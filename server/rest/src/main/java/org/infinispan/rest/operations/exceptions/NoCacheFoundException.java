package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NoCacheFoundException extends RestResponseException {

   public NoCacheFoundException(String description) {
      super(HttpResponseStatus.NOT_FOUND, description);
   }

}
