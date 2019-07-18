package org.infinispan.client.rest;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.util.Experimental;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Experimental
public interface RestResponse {
   int getStatus();

   String getBody();

   MediaType contentType();

   Protocol getProtocol();
}
