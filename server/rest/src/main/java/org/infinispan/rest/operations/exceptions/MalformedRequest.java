package org.infinispan.rest.operations.exceptions;

import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 9.2
 */
public class MalformedRequest extends RestResponseException {

   public MalformedRequest(String description) {
      super(HttpResponseStatus.BAD_REQUEST, description);
   }
}
