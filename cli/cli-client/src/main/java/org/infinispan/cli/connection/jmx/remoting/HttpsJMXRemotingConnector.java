package org.infinispan.cli.connection.jmx.remoting;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.Connector;
import org.infinispan.cli.connection.jmx.JMXConnection;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@SuppressWarnings("unused")
public class HttpsJMXRemotingConnector implements Connector {

   public HttpsJMXRemotingConnector() {
   }

   @Override
   public Connection getConnection(final String connectionString) {
      try {
         return new JMXConnection(new HttpsJMXRemotingUrl(connectionString));
      } catch (Exception e) {
         return null;
      }
   }

}
