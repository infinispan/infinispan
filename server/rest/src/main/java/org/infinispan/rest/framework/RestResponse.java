package org.infinispan.rest.framework;

/**
 * @since 10.0
 */
public interface RestResponse {

   int getStatus();

   Object getEntity();
}
