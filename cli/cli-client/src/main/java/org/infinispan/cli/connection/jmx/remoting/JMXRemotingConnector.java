package org.infinispan.cli.connection.jmx.remoting;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.Connector;
import org.infinispan.cli.connection.jmx.JMXConnection;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class JMXRemotingConnector implements Connector {

   public JMXRemotingConnector() {
   }

   @Override
   public Connection getConnection(final String connectionString) {
      try {
         return new JMXConnection(new JMXRemotingUrl(connectionString));
      } catch (Exception e) {
         return null;
      }
   }

}
