package org.infinispan.cli.connection.jmx.rmi;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.Connector;
import org.infinispan.cli.connection.jmx.JMXConnection;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@SuppressWarnings("unused")
public class JMXRMIConnector implements Connector {

   public JMXRMIConnector() {
   }

   @Override
   public Connection getConnection(final String connectionString) {
      try {
         return new JMXConnection(new JMXRMIUrl(connectionString));
      } catch (Exception e) {
         return null;
      }
   }


}
