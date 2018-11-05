package org.infinispan.rest.framework.impl;

import java.util.Map;

import org.infinispan.rest.framework.ContentSource;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RestRequest;

/**
 * @since 10.0
 */
public interface RestRequestBuilder<B extends RestRequestBuilder<B>> {

   B setMethod(Method method);

   B setPath(String path);

   B setHeaders(Map<String, String> headers);

   B setContents(ContentSource contents);

   RestRequest build();

}
