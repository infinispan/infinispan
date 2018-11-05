package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ResourceNotFoundException extends RestResponseException {

   public ResourceNotFoundException() {
      super(HttpResponseStatus.NOT_FOUND, "Resource not found");
   }

}
