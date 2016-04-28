package org.infinispan.cli.connection.jmx.remoting;

/**
 * DirectJMXRemotingUrl connects through plain (non-http-upgrade-enabled) ports
 *
 * @author Tristan Tarrant
 * @since 9.0
 */

public class DirectJMXRemotingUrl extends JMXRemotingUrl {
   public DirectJMXRemotingUrl(String connectionString) {
      super(connectionString);
   }

   @Override
   String getProtocol() {
      return "remoting";
   }

   @Override
   int getDefaultPort() {
      return 9999;
   }
}
