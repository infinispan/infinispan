package org.infinispan.cli.connection.jmx.remoting;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.Connector;
import org.infinispan.cli.connection.jmx.JMXConnection;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@SuppressWarnings("unused")
public class DirectJMXRemotingConnector implements Connector {

   public DirectJMXRemotingConnector() {
   }

   @Override
   public Connection getConnection(final String connectionString) {
      try {
         return new JMXConnection(new DirectJMXRemotingUrl(connectionString));
      } catch (Exception e) {
         return null;
      }
   }

}
