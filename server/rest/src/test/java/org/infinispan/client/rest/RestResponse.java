package org.infinispan.client.rest;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestResponse {
   int getStatus();

   String getBody();
}
