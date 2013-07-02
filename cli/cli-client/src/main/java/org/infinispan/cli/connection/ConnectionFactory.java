package org.infinispan.cli.connection;

import java.util.ServiceLoader;

public final class ConnectionFactory {
   public static Connection getConnection(final String connectionString) {
      ServiceLoader<Connector> connectors = ServiceLoader.load(Connector.class);
      for (Connector connector : connectors) {
         Connection connection = connector.getConnection(connectionString);
         if (connection != null) {
            return connection;
         }
      }
      return null;
   }

}
