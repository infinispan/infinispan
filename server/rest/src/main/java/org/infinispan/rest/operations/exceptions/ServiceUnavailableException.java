package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ServiceUnavailableException extends RestResponseException {

   public ServiceUnavailableException(String text) {
      super(HttpResponseStatus.SERVICE_UNAVAILABLE, text);
   }

}
