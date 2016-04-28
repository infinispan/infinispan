package org.infinispan.cli.connection.jmx.remoting;

/**
 * HttpsJMXRemotingUrl connects through HTTPS ports
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class HttpsJMXRemotingUrl extends JMXRemotingUrl {
   public HttpsJMXRemotingUrl(String connectionString) {
      super(connectionString);
   }

   @Override
   String getProtocol() {
      return "https-remoting";
   }

   @Override
   int getDefaultPort() {
      return 9990;
   }
}
