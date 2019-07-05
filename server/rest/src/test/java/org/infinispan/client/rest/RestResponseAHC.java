package org.infinispan.client.rest;

import org.asynchttpclient.Response;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestResponseAHC implements RestResponse{
   Response response;

   RestResponseAHC(Response response) {
      this.response = response;
   }

   public int getStatus() {
      return response.getStatusCode();
   }

   public String getBody() {
      return response.getResponseBody();
   }
}
