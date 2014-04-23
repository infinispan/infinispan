package org.infinispan.cli.connection;

import org.infinispan.commons.util.ServiceFinder;

public final class ConnectionFactory {
   public static Connection getConnection(final String connectionString) {
      for (Connector connector : ServiceFinder.load(Connector.class)) {
         Connection connection = connector.getConnection(connectionString);
         if (connection != null) {
            return connection;
         }
      }
      return null;
   }

}
