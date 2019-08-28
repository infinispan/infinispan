package org.infinispan.client.rest;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestResponse {
   int getStatus();

   String getBody();

   MediaType contentType();
}
