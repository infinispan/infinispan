package org.infinispan.client.rest;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.commons.util.Experimental;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Experimental
public interface RestResponse extends RestEntity, AutoCloseable {
   int OK = 200;
   int CREATED = 201;
   int ACCEPTED = 202;
   int NO_CONTENT = 204;
   int NOT_MODIFIED = 304;
   int TEMPORARY_REDIRECT = 307;
   int BAD_REQUEST = 400;
   int UNAUTHORIZED = 401;
   int FORBIDDEN = 403;
   int NOT_FOUND = 404;
   int METHOD_NOT_ALLOWED = 405;
   int CONFLICT = 409;
   int INTERNAL_SERVER_ERROR = 500;
   int SERVICE_UNAVAILABLE = 503;

   int getStatus();

   Map<String, List<String>> headers();

   /**
    * Returns the value of a header as a String. For multi-valued headers, values are separated by comma.
    */
   String getHeader(String header);

   InputStream getBodyAsStream();

   byte[] getBodyAsByteArray();

   Protocol getProtocol();

   void close();

   boolean usedAuthentication();
}
