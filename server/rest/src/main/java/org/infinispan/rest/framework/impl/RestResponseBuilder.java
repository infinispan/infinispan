package org.infinispan.rest.framework.impl;

import java.util.Date;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.framework.RestResponse;

/**
 * @since 10.0
 */
public interface RestResponseBuilder<B extends RestResponseBuilder<B>> {

   RestResponse build();

   B status(int status);

   B entity(Object entity);

   B cacheControl(CacheControl cacheControl);

   B header(String name, Object value);

   B contentType(MediaType type);

   B expires(Date expires);

   B lastModified(Date lastModified);

   B eTag(String tag);

   int getStatus();

   Object getEntity();

   Object getHeader(String header);
}
