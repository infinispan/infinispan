package org.infinispan.cli.connection.jmx.remoting;

/**
 * HttpJMXRemotingUrl connects through HTTP ports
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class HttpJMXRemotingUrl extends JMXRemotingUrl {
   public HttpJMXRemotingUrl(String connectionString) {
      super(connectionString);
   }

   @Override
   String getProtocol() {
      return "http-remoting";
   }

   @Override
   int getDefaultPort() {
      return 9990;
   }
}
