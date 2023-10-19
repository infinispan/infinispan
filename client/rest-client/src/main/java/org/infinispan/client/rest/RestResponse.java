package org.infinispan.client.rest;

import java.io.InputStream;

import org.infinispan.commons.util.Experimental;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Experimental
public interface RestResponse extends RestResponseInfo, AutoCloseable {

   String body();

   InputStream bodyAsStream();

   byte[] bodyAsByteArray();

   void close();

   boolean usedAuthentication();
}
