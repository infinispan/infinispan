package org.infinispan.rest.framework.impl;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.framework.RestResponse;

/**
 * @since 10.0
 */
public interface RestResponseBuilder<B extends RestResponseBuilder<B>> {

   RestResponse build();


   default CompletionStage<RestResponse> buildFuture() {
      return CompletableFuture.completedFuture(build());
   }

   B status(int status);

   B entity(Object entity);

   B cacheControl(CacheControl cacheControl);

   B header(String name, Object value);

   B contentType(MediaType type);

   B contentType(String type);

   B contentLength(long length);

   B expires(Date expires);

   B lastModified(Long epoch);

   B location(String location);

   B addProcessedDate(Date d);

   B eTag(String tag);

   int getStatus();

   Object getEntity();

   Object getHeader(String header);
}
