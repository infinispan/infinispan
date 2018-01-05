package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class UnacceptableDataFormatException extends RestResponseException {

   public UnacceptableDataFormatException() {
      super(HttpResponseStatus.NOT_ACCEPTABLE, "Data format not supported");
   }

   public UnacceptableDataFormatException(String reason) {
      super(HttpResponseStatus.NOT_ACCEPTABLE, reason);
   }
}
