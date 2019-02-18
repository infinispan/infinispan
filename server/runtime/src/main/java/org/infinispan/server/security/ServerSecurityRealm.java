package org.infinispan.server.security;

import javax.net.ssl.SSLContext;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerSecurityRealm {
   final String name;

   public ServerSecurityRealm(String name) {
      this.name = name;
   }

   /**
    * Retrieves the SSL context for this realm. If one has not been defined, this will throw an {@link IllegalStateException}
    */
   SSLContext getSSLContext() {
      throw new IllegalStateException("SSLContext request for realm " + name + " but no key/trust stores defined");
   }
}
