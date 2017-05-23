package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class PreconditionsFailedException extends RestResponseException {

   public PreconditionsFailedException() {
      super(HttpResponseStatus.PRECONDITION_FAILED, null);
   }

}
